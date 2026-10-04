//! Handler for `InputLayer`
//!
//! Core business logic for IQL queries and data operations, used by the HTTP/WebSocket API.
//! Uses `parking_lot::RwLock` (no poisoning) and `AtomicU64` (lock-free counters).
//!
//! # Error handling
//!
//! Production code in this module avoids `unwrap()` entirely - all fallible operations
//! use `?`, `map_err()`, `unwrap_or()`, or `unwrap_or_default()` for proper error propagation.
//! Test code uses `expect()` with descriptive messages for better failure diagnostics.

use crate::ast::Term;
use crate::execution::{QueryMemoryPool, RequestControl};
use crate::index_manager::IndexStats;
use crate::rule_catalog::validate_rule;

use crate::session::{SessionConfig, SessionId, SessionManager};
use crate::statement;
use crate::statement::meta::{IndexCreateOptions, MetaCommand};
use crate::statement::parser::SortDirection;
use crate::storage_engine::{KnowledgeGraphSnapshot, StorageEngine};
use crate::value::{Tuple, Value};
use crate::Config;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

use super::notification_log::NotificationLog;
use super::wire::{
    ColumnDef, ErrorCode, QueryResult, StatementError, WireDataType, WireTuple, WireValue,
};
use catalog_staging::CatalogStatement;
use fact_staging::FactStatement;
pub use inputlayer_ws_protocol::{Notification, ValidationError};
use write_run::{WriteRun, WriteStatement};

mod backup_command;
mod catalog_staging;
mod fact_staging;
mod program_boundary;
mod supervise;
pub(crate) use supervise::stop_error;
mod write_run;

/// Result of transforming a `?shorthand` query, including sort and pagination annotations.
pub(crate) struct QueryTransform {
    /// The transformed query program text.
    pub query: String,
    /// Column-index-based sort specification, extracted from `:asc`/`:desc` annotations.
    pub order_by: Vec<(usize, SortDirection)>,
    /// Maximum number of rows to return.
    pub limit: Option<usize>,
    /// Number of rows to skip before applying limit.
    pub offset: Option<usize>,
    /// The `__query__` head: one variable per result column (empty when the
    /// text was not a `?` query).
    pub columns: Vec<String>,
}

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

/// Reply when `.subscribe`/`.unsubscribe` reach the generic executor.
const SUBSCRIPTION_WS_ONLY: &str =
    ".subscribe and .unsubscribe are only available as standalone commands on the global /ws endpoint.";

/// Standing-query sharing probes computing at once, server-wide.
const PROBE_PERMITS: usize = 1;

/// Password logins allowed in flight; more are refused as busy.
const MAX_QUEUED_LOGINS: usize = 64;

/// Prefix used to encode structured validation errors in error strings.
/// WebSocket handlers can detect this prefix to extract per-line error info.
pub const VALIDATION_ERROR_PREFIX: &str = "VALIDATION_ERRORS:";

/// A program that failed as a whole.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ProgramError {
    pub message: String,
    /// Set when the program's only statement failed.
    pub code: Option<ErrorCode>,
}

impl From<crate::storage::StorageError> for ProgramError {
    fn from(error: crate::storage::StorageError) -> Self {
        Self {
            code: Some(storage_error_code(&error, ErrorCode::Internal)),
            message: error.to_string(),
        }
    }
}

impl From<String> for ProgramError {
    fn from(message: String) -> Self {
        Self {
            message,
            code: None,
        }
    }
}

/// Whether `stmt`, the only statement of its program, changes state in the
/// intercepts of `run_execute_program` rather than in the program executor:
/// ontology, user, key and access management, and (on a session) session
/// state.
fn intercepted_mutation(stmt: &statement::Statement, on_session: bool) -> bool {
    use statement::Statement;
    match stmt {
        Statement::Meta(command) => matches!(
            command,
            MetaCommand::OntologyInstall(_)
                | MetaCommand::OntologyRemove(_)
                | MetaCommand::OntologyUpgrade(_)
                | MetaCommand::UserCreate { .. }
                | MetaCommand::UserDrop(_)
                | MetaCommand::UserPassword { .. }
                | MetaCommand::UserRole { .. }
                | MetaCommand::ApiKeyCreate { .. }
                | MetaCommand::ApiKeyRevoke(_)
                | MetaCommand::ApiKeyExpire { .. }
                | MetaCommand::KgAclGrant { .. }
                | MetaCommand::KgAclRevoke { .. }
                | MetaCommand::SessionClear
                | MetaCommand::SessionDrop(_)
                | MetaCommand::SessionDropName(_)
        ),
        Statement::SessionRule(_) | Statement::Fact(_) => on_session,
        _ => false,
    }
}

impl From<ProgramError> for String {
    fn from(error: ProgramError) -> Self {
        error.message
    }
}

/// Thread-safe wrapper around StorageEngine for concurrent API calls.
/// Per-KG schema validation via isolated SchemaCatalogs.
///
/// Includes a `SessionManager` for ephemeral triggers persistent: sessions
/// can inject ephemeral facts/rules that combine with persistent data for queries.
pub struct Handler {
    storage: Arc<RwLock<StorageEngine>>,
    /// Cached copy of the engine configuration for fast access in hot paths.
    config: Arc<crate::Config>,
    start_time: Instant,
    query_count: Arc<AtomicU64>,
    insert_count: Arc<AtomicU64>,
    /// Session manager for ephemeral state
    sessions: SessionManager,
    /// The ordered stream of committed-change notifications.
    notifications: Arc<NotificationLog>,
    /// Semaphore limiting concurrent DD computations.
    /// Prevents blocking-thread-pool explosion by capping CPU-bound parallelism
    /// at the hardware thread count. Tokio workers queue via async `acquire()`.
    query_semaphore: Arc<tokio::sync::Semaphore>,
    /// Width of `query_semaphore`: queries that run at once.
    compute_permits: usize,
    /// Permits of standing-query sharing probes, apart from the compute
    /// permits so that a probe never takes one a query waits for.
    probe_semaphore: Arc<tokio::sync::Semaphore>,
    /// Memory held by the computations of every request in flight.
    query_memory: Arc<QueryMemoryPool>,
    /// Accumulated timing histogram buckets for Prometheus export.
    timing_histograms: Arc<crate::execution::timing::TimingHistograms>,
    /// Standing-query counters (evaluations, active subscriptions, views).
    subscription_metrics: Arc<super::subscription::SubscriptionMetrics>,
    /// The worker owning the shared standing-query views, started by the
    /// first subscription.
    subscription_hub: std::sync::OnceLock<super::subscription::SubscriptionHub>,
    /// Login failure counters.
    login_throttle: Arc<crate::auth::LoginThrottle>,
    /// Caps concurrent argon2 verifications.
    password_permits: Arc<tokio::sync::Semaphore>,
    /// Caps logins in flight, including those waiting for `password_permits`.
    login_queue: Arc<tokio::sync::Semaphore>,
    /// Live users and API keys; sessions hold principals issued from it.
    credentials: crate::auth::CredentialRegistry,
    /// Serializes credential mutations across validation, storage and
    /// registry publication. A dedicated lock, so queries, proofs and replay
    /// holding the storage read guard never delay a revocation.
    credential_writes: parking_lot::Mutex<()>,
    /// Bumped after every change to the KG access lists (`kg_acls`).
    kg_acl_generation: AtomicU64,
}

/// `.index` command implementations shared by `Handler` and `QueryJob`.
mod index_commands {
    use super::ProgramError;
    use crate::index_manager::IndexStats;
    use crate::statement::meta::IndexCreateOptions;
    use crate::StorageEngine;

    pub(super) fn create(
        storage: &StorageEngine,
        kg: &str,
        opts: &IndexCreateOptions,
    ) -> Result<String, ProgramError> {
        let stats = storage
            .create_index_in(kg, opts)
            .map_err(ProgramError::from)?;
        Ok(format!(
            "Index '{}' created on {}.{} ({} vectors).",
            stats.name, stats.relation, stats.column, stats.tuple_count
        ))
    }

    pub(super) fn drop(
        storage: &StorageEngine,
        kg: &str,
        name: &str,
    ) -> Result<String, ProgramError> {
        storage
            .drop_index_in(kg, name)
            .map_err(ProgramError::from)?;
        Ok(format!("Index '{name}' dropped."))
    }

    pub(super) fn rebuild(
        storage: &StorageEngine,
        kg: &str,
        name: &str,
    ) -> Result<String, ProgramError> {
        let stats = storage
            .rebuild_index_in(kg, name)
            .map_err(ProgramError::from)?;
        Ok(format!(
            "Index '{name}' rebuilt ({} vectors).",
            stats.tuple_count
        ))
    }

    pub(super) fn stats(
        storage: &StorageEngine,
        kg: &str,
        name: Option<&str>,
    ) -> Result<Vec<IndexStats>, String> {
        storage.index_stats_in(kg, name).map_err(|e| e.to_string())
    }
}

/// Label of the API key admin bootstrap issues.
const BOOTSTRAP_KEY_LABEL: &str = "bootstrap";

fn bootstrap_key_row() -> Tuple {
    Tuple::new(vec![Value::string(BOOTSTRAP_KEY_LABEL)])
}

/// Record that admin bootstrap has issued its API key, so it never issues
/// another.
fn record_bootstrap_key(storage: &StorageEngine) -> Result<(), String> {
    storage
        .insert_tuples_into(
            crate::auth::INTERNAL_KG,
            crate::auth::stored::BOOTSTRAP_KEYS,
            vec![bootstrap_key_row()],
        )
        .map(drop)
        .map_err(|e| e.to_string())
}

/// Store `key` as admin's bootstrap API key together with the record of its
/// issue, as one program: either both are stored or neither is.
fn store_bootstrap_key(storage: &StorageEngine, key: &str) -> Result<(), String> {
    use crate::auth;
    use crate::storage_engine::{FactChange, StagedChanges, WriteProgram};

    let record = auth::ApiKeyRecord {
        label: BOOTSTRAP_KEY_LABEL.to_string(),
        key_hash: auth::hash_api_key(key),
        username: "admin".to_string(),
        times: auth::ApiKeyTimes {
            created_at: Some(now_ms()),
            ..auth::ApiKeyTimes::default()
        },
        scope: None,
    };
    let mut changes = vec![FactChange::Insert {
        relation: auth::stored::BOOTSTRAP_KEYS.to_string(),
        tuples: vec![bootstrap_key_row()],
    }];
    changes.extend(api_keys::api_key_inserts(&record));
    storage
        .commit_program(
            auth::INTERNAL_KG,
            WriteProgram::single(StagedChanges::Facts(changes)),
            None,
        )
        .map(drop)
        .map_err(|e| e.into_storage_error().to_string())
}

/// Whether `_internal` holds a user named `username`.
fn user_exists(snapshot: &KnowledgeGraphSnapshot, username: &str) -> bool {
    snapshot
        .input_tuples
        .get("users")
        .into_iter()
        .flatten()
        .any(|t| t.values().first().and_then(Value::as_str) == Some(username))
}

/// `kg_acl_relations(kg, username, relations)`: the relations a `writer` or
/// `decider` grant in `kg_acls` may write, as `auth::decode_relations` reads
/// them. A `writer` grant with no row writes every relation.
const KG_ACL_RELATIONS: &str = "kg_acl_relations";

/// The access a `kg_acls` grant of `role` gives `username` on `kg_name`. A
/// writer or decider grant whose relations do not read back as exactly one
/// list gives no access, rather than more than was granted.
fn kg_access_from_acl(
    snapshot: &KnowledgeGraphSnapshot,
    kg_name: &str,
    username: &str,
    role: crate::auth::KgRole,
) -> Option<crate::auth::KgAccess> {
    use crate::auth::{self, KgAccess, KgRole};

    if !matches!(role, KgRole::Writer | KgRole::Decider) {
        return Some(role.into());
    }
    let mut rows = snapshot
        .input_tuples
        .get(KG_ACL_RELATIONS)
        .into_iter()
        .flatten()
        .filter(|tuple| {
            let values = tuple.values();
            values.first().and_then(Value::as_str) == Some(kg_name)
                && values.get(1).and_then(Value::as_str) == Some(username)
        });
    match (rows.next(), rows.next()) {
        (None, _) => Some(role.into()),
        (Some(row), None) => {
            let stored = row.values().get(2).and_then(Value::as_str)?;
            KgAccess::new(role, auth::decode_relations(stored).ok()?).ok()
        }
        (Some(_), Some(_)) => None,
    }
}

/// Deletions of every `kg_acls` and `kg_acl_relations` row whose KG and user
/// `matches` selects; empty when there is none.
fn acl_grant_deletes(
    snapshot: &KnowledgeGraphSnapshot,
    matches: impl Fn(&str, &str) -> bool,
) -> Vec<crate::storage_engine::FactChange> {
    ["kg_acls", KG_ACL_RELATIONS]
        .into_iter()
        .filter_map(|relation| {
            let tuples: Vec<Tuple> = snapshot
                .input_tuples
                .get(relation)
                .into_iter()
                .flatten()
                .filter(|tuple| match tuple.values() {
                    [kg, user, ..] => kg
                        .as_str()
                        .zip(user.as_str())
                        .is_some_and(|(kg, user)| matches(kg, user)),
                    _ => false,
                })
                .cloned()
                .collect();
            (!tuples.is_empty()).then(|| crate::storage_engine::FactChange::Delete {
                relation: relation.to_string(),
                tuples,
            })
        })
        .collect()
}

/// Apply `changes` to `_internal` as one commit: all of them or none.
fn commit_internal(
    storage: &StorageEngine,
    changes: Vec<crate::storage_engine::FactChange>,
) -> Result<(), crate::storage::StorageError> {
    use crate::storage_engine::{StagedChanges, WriteProgram};

    storage
        .commit_program(
            crate::auth::INTERNAL_KG,
            WriteProgram::single(StagedChanges::Facts(changes)),
            None,
        )
        .map(drop)
        .map_err(|e| e.into_storage_error())
}

/// Current epoch milliseconds.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Reject a session rule that puts negation inside a recursive cycle,
/// checked against the KG's persistent rules and the session's rules.
fn validate_session_rule_stratification(
    storage: &StorageEngine,
    kg: Option<&str>,
    session_rules: &[crate::ast::Rule],
    rule: &crate::ast::Rule,
) -> Result<(), String> {
    let kg = kg.or_else(|| storage.current_knowledge_graph());
    let mut rules: Vec<crate::ast::Rule> = kg
        .and_then(|kg| storage.get_snapshot_for(kg).ok())
        .map(|snapshot| snapshot.rules.to_vec())
        .unwrap_or_default();
    rules.extend(session_rules.iter().cloned());
    rules.push(rule.clone());
    crate::rule_catalog::validate_rules_stratification(&rules)
}

/// Every check a session rule must pass before it joins `session_rules`.
fn check_session_rule(
    storage: &StorageEngine,
    kg: Option<&str>,
    session_rules: &[crate::ast::Rule],
    rule: &crate::ast::Rule,
) -> Result<(), String> {
    // The head may carry a leading '~' (transient marker).
    if rule.head.relation.trim_start_matches('~').starts_with("__") {
        return Err(format!(
            "Session rule '{}' uses reserved '__' prefix. Choose a different relation name.",
            rule.head.relation
        ));
    }
    validate_rule(rule, &rule.head.relation)?;
    crate::rule_catalog::validate_session_rule_compatibility(session_rules, rule)?;
    validate_session_rule_stratification(storage, kg, session_rules, rule)
}

/// The tuple of a session fact.
fn session_fact_tuple(rule: &crate::ast::Rule) -> Result<Tuple, String> {
    if rule.head.args.is_empty() {
        return Err("Fact must have at least one argument".to_string());
    }
    let values: Result<Vec<Value>, String> = rule.head.args.iter().map(term_to_value).collect();
    values.map(Tuple::new)
}

/// Compile a query (parse, IR, optimize) without executing it; returns the
/// formatted plan trace and the optimization passes applied.
fn debug_query(
    storage: &StorageEngine,
    knowledge_graph: Option<&str>,
    query: &str,
) -> Result<(String, Vec<String>), String> {
    let kg_name = if let Some(kg) = knowledge_graph {
        storage
            .ensure_knowledge_graph(kg)
            .map_err(|e| format!("Knowledge graph not found: {e}"))?;
        kg.to_string()
    } else {
        storage
            .current_knowledge_graph()
            .ok_or("No knowledge graph selected")?
            .to_string()
    };

    let trace = storage
        .debug_query_on(&kg_name, query)
        .map_err(|e| format!("{e}"))?;

    let optimizations = storage
        .config()
        .optimization
        .enabled_passes()
        .into_iter()
        .map(String::from)
        .collect();

    Ok((trace.format_trace(), optimizations))
}

/// Test seams: a hook runs once, on the executing thread, when
/// `QueryJob::execute` reaches its point. `Handler::query_program` carries
/// hooks set on the calling thread to the blocking thread that runs the job.
#[cfg(test)]
mod test_hook {
    use std::cell::RefCell;
    use std::collections::HashMap;

    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    pub(super) enum Point {
        /// Just before a meta command is dispatched, while the storage read
        /// guard is held.
        MetaDispatch,
        /// After a proof captured its snapshot and released the guard, before
        /// proof search.
        ProofSearch,
        /// After a program's writes staged, before they commit.
        Commit,
    }

    type Hooks = HashMap<Point, Box<dyn FnOnce() + Send>>;

    thread_local! {
        static HOOKS: RefCell<Hooks> = RefCell::default();
    }

    pub(super) fn set(point: Point, hook: impl FnOnce() + Send + 'static) {
        HOOKS.with(|hooks| hooks.borrow_mut().insert(point, Box::new(hook)));
    }

    pub(super) fn run(point: Point) {
        if let Some(hook) = HOOKS.with(|hooks| hooks.borrow_mut().remove(&point)) {
            hook();
        }
    }

    pub(super) fn take() -> Hooks {
        HOOKS.with(|hooks| std::mem::take(&mut *hooks.borrow_mut()))
    }

    /// Installs `hooks` on this thread until the returned guard drops, so an
    /// unused hook never reaches a later job on a pooled thread.
    pub(super) fn install(hooks: Hooks) -> Installed {
        HOOKS.with(|h| *h.borrow_mut() = hooks);
        Installed
    }

    pub(super) struct Installed;

    impl Drop for Installed {
        fn drop(&mut self) {
            drop(take());
        }
    }
}

mod api_keys;

#[cfg(test)]
mod guard_reentry_tests;

#[cfg(test)]
mod proof_snapshot_tests;

#[cfg(test)]
mod pinned_proof_tests;

#[cfg(test)]
mod revocation_tests;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod credential_mutation_tests;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod scoped_roles_tests;

/// Self-contained snapshot of Handler state for executing a single query on a blocking thread.
/// All fields are `Arc`-wrapped (`Send + Sync`), allowing the job to be moved into
/// `tokio::task::spawn_blocking` without holding any `!Send` lock guards across `.await` points.
struct QueryJob {
    storage: Arc<RwLock<StorageEngine>>,
    config: Arc<crate::Config>,
    notifications: Arc<NotificationLog>,
    insert_count: Arc<AtomicU64>,
    query_count: Arc<AtomicU64>,
    start_time: Instant,
    timing_histograms: Arc<crate::execution::timing::TimingHistograms>,
    /// When set, the query reads this snapshot of its knowledge graph instead
    /// of the one current when it runs.
    pinned: Option<Arc<KnowledgeGraphSnapshot>>,
    /// When set, reuse the query's compiled plan while the rules stay the
    /// same (for queries evaluated again and again: standing queries), and
    /// note here how the run went.
    cache_plan: Option<Arc<std::sync::OnceLock<crate::storage_engine::CachedRun>>>,
}

impl QueryJob {
    fn inc_query_count(&self) {
        self.query_count.fetch_add(1, Ordering::Relaxed);
    }

    fn total_queries(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }

    fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    fn send_notification(&self, notification: Notification) {
        self.notifications.publish(notification);
    }

    fn notify_persistent_update(&self, kg: &str, relation: &str, operation: &str, count: usize) {
        self.notify_persistent_update_with_session(kg, relation, operation, count, None);
    }

    fn notify_persistent_update_with_session(
        &self,
        kg: &str,
        relation: &str,
        operation: &str,
        count: usize,
        session_id: Option<String>,
    ) {
        self.send_notification(Notification::PersistentUpdate {
            knowledge_graph: kg.to_string(),
            relation: relation.to_string(),
            operation: operation.to_string(),
            count,
            timestamp_ms: now_ms(),
            session_id,
            seq: 0, // placeholder - set by send_notification
        });
    }

    fn notify_rule_change(&self, kg: &str, rule_name: &str, operation: &str) {
        self.send_notification(Notification::RuleChange {
            knowledge_graph: kg.to_string(),
            rule_name: rule_name.to_string(),
            operation: operation.to_string(),
            timestamp_ms: now_ms(),
            seq: 0,
        });
    }

    fn notify_kg_change(&self, kg: &str, operation: &str) {
        self.send_notification(Notification::KgChange {
            knowledge_graph: kg.to_string(),
            operation: operation.to_string(),
            timestamp_ms: now_ms(),
            seq: 0,
        });
    }

    fn notify_schema_change(&self, kg: &str, entity: &str, operation: &str) {
        self.send_notification(Notification::SchemaChange {
            knowledge_graph: kg.to_string(),
            entity: entity.to_string(),
            operation: operation.to_string(),
            timestamp_ms: now_ms(),
            seq: 0,
        });
    }
}

/// The storage state a `.why` or `.why_not` proof reads, captured under the
/// caller's storage guard.
///
/// Proof search runs on this snapshot alone, so the caller releases its guard
/// before the costly evaluation instead of re-acquiring storage inside it.
struct ProofSnapshot {
    snapshot: Arc<KnowledgeGraphSnapshot>,
    index_metrics: std::collections::HashMap<String, String>,
}

impl ProofSnapshot {
    fn capture(storage: &StorageEngine, kg: &str) -> Result<Self, String> {
        storage
            .ensure_knowledge_graph(kg)
            .map_err(|e| format!("Knowledge graph not found: {e}"))?;
        let (snapshot, index_metrics) = storage
            .proof_snapshot_on(kg)
            .map_err(|e| format!("Failed to access knowledge graph: {e}"))?;
        Ok(Self {
            snapshot,
            index_metrics,
        })
    }

    /// The proof state of a program whose writes committed against `base`
    /// (see [`ProgramCommit::base`](crate::storage_engine::ProgramCommit)).
    fn pinned(
        storage: &StorageEngine,
        kg: &str,
        base: Arc<KnowledgeGraphSnapshot>,
    ) -> Result<Self, String> {
        let index_metrics = storage
            .index_metrics_on(kg)
            .map_err(|e| format!("Failed to access knowledge graph: {e}"))?;
        Ok(Self {
            snapshot: base,
            index_metrics,
        })
    }

    /// Evaluate `proof` on this snapshot.
    fn explain(
        self,
        proof: Proof,
        timing_mode: crate::execution::TimingMode,
    ) -> Result<QueryResult, String> {
        match proof {
            Proof::Why { query, full } => self.why(&query, full, timing_mode),
            Proof::WhyNot(input) => self.why_not(&input, timing_mode),
        }
    }

    /// Build proof trees explaining why query results were derived.
    ///
    /// Returns a QueryResult with both the result rows AND proof trees
    /// in the `proof_trees` field, so clients get typed data not text.
    fn why(
        self,
        query: &str,
        full_mode: bool,
        timing_mode: crate::execution::TimingMode,
    ) -> Result<QueryResult, String> {
        use crate::provenance::backward_chaining::{build_proof_tree, ProofContext};
        use crate::provenance::ProofConfig;

        let start = std::time::Instant::now();
        let query_start = std::time::Instant::now();
        let (mut result_tuples, derived_data) = self
            .snapshot
            .execute_with_rules_tuples_and_derived(query)
            .map_err(|e| format!("Query execution failed: {e}"))?;
        let row_capped = crate::last_result_truncated();
        let query_us = query_start.elapsed().as_micros() as u64;

        if result_tuples.is_empty() {
            return Ok(QueryResult::empty());
        }

        result_tuples.sort();

        let relation = extract_query_relation(query)
            .ok_or_else(|| "Could not determine query relation name".to_string())?;
        let config = ProofConfig {
            full_mode,
            ..ProofConfig::default()
        };

        // Build index proof info from metrics
        let index_info: std::collections::HashMap<
            String,
            crate::provenance::backward_chaining::IndexProofInfo,
        > = self
            .index_metrics
            .into_iter()
            .map(|(name, metric)| {
                (
                    name,
                    crate::provenance::backward_chaining::IndexProofInfo {
                        metric,
                        query_vector: Vec::new(),
                    },
                )
            })
            .collect();
        let ctx = ProofContext::with_index_info(
            &self.snapshot.rules,
            &self.snapshot.input_tuples,
            config.clone(),
            index_info,
        )
        .with_derived_data(&derived_data);

        // Build wire rows and proof trees
        let schema = extract_query_schema(query, &result_tuples);
        let mut rows = Vec::new();
        let mut graphs = Vec::new();

        let proof_start = std::time::Instant::now();
        for tuple in &result_tuples {
            let values: Vec<WireValue> = (0..tuple.arity())
                .filter_map(|i| tuple.get(i).map(WireValue::from_value))
                .collect();
            rows.push(WireTuple::new(values));

            let mut graph = match build_proof_tree(&relation, tuple, &ctx) {
                Ok(g) => g,
                Err(err) => {
                    tracing::warn!(relation = %relation, error = %err, "build_proof_tree failed");
                    // Fallback: single truncated node
                    use crate::provenance::proof_tree::*;
                    let mut builder = ProofTreeBuilder::new();
                    let id = builder.insert_unique(ProofNode {
                        kind: NodeKind::Truncated,
                        conclusion: Conclusion {
                            pred: relation.clone(),
                            args: (0..tuple.arity())
                                .filter_map(|i| tuple.get(i).cloned())
                                .collect(),
                        },
                        rule_id: None,
                        bindings: None,
                        aggregate: None,
                        negation: None,
                        vector_search: None,
                        truncated: Some(TruncatedInfo {
                            depth_limit: config.max_depth,
                        }),
                        why_not: None,
                        source: None,
                        children: vec![],
                    });
                    builder.finish(vec![id])
                }
            };
            graph.query = Some(query.to_string());
            graph.revision = Some(self.snapshot.revision);
            graphs.push(graph);
        }
        let proof_us = proof_start.elapsed().as_micros() as u64;

        let total_count = rows.len();
        Ok(QueryResult {
            rows,
            schema,
            total_count,
            truncated: row_capped,
            execution_time_ms: start.elapsed().as_millis() as u64,
            metadata: None,
            switched_kg: None,
            proof_trees: Some(graphs),
            timing_breakdown: proof_timing(
                timing_mode,
                start,
                query_us,
                "proof_tree_construction",
                proof_us,
            ),
            errors: Vec::new(),
            statements: Vec::new(),
            revision: None,
        })
    }

    /// Explain why a specific tuple was NOT derived.
    ///
    /// Returns a QueryResult with the explanation as structured proof tree.
    fn why_not(
        self,
        input: &str,
        timing_mode: crate::execution::TimingMode,
    ) -> Result<QueryResult, String> {
        use crate::provenance::backward_chaining::ProofContext;
        use crate::provenance::why_not::{explain_why_not, format_why_not_text};
        use crate::provenance::ProofConfig;

        let start = std::time::Instant::now();
        let (relation, tuple) = parse_why_not_target(input)?;
        let query_start = std::time::Instant::now();
        let ctx = ProofContext::new(
            &self.snapshot.rules,
            &self.snapshot.input_tuples,
            ProofConfig::default(),
        );
        let query_us = query_start.elapsed().as_micros() as u64;

        let explain_start = std::time::Instant::now();
        // Build the rich proof tree (for structured export/GUI)
        let mut graph = explain_why_not(&relation, &tuple, &ctx);
        graph.query = Some(format!(".why_not {input}"));
        graph.revision = Some(self.snapshot.revision);
        let explain_us = explain_start.elapsed().as_micros() as u64;

        // Derive text from the graph (no duplicated logic)
        let formatted = format_why_not_text(&graph);
        let rows: Vec<WireTuple> = formatted
            .lines()
            .map(|line| WireTuple::new(vec![WireValue::String(line.to_string())]))
            .collect();
        let total_count = rows.len();

        Ok(QueryResult {
            rows,
            schema: vec![ColumnDef::string("explanation")],
            total_count,
            truncated: false,
            execution_time_ms: start.elapsed().as_millis() as u64,
            metadata: None,
            switched_kg: None,
            proof_trees: Some(vec![graph]),
            timing_breakdown: proof_timing(timing_mode, start, query_us, "explanation", explain_us),
            errors: Vec::new(),
            statements: Vec::new(),
            revision: None,
        })
    }
}

/// A proof command: `.why` (`full` for `.why full`) or `.why_not`.
enum Proof {
    Why { query: String, full: bool },
    WhyNot(String),
}

impl Proof {
    /// The prefix of the proof's error message.
    fn label(&self) -> &'static str {
        match self {
            Self::Why { .. } => "Why error",
            Self::WhyNot(_) => "Why-not error",
        }
    }
}

/// Timing for a proof command: query evaluation, then the named proof phase.
fn proof_timing(
    timing_mode: crate::execution::TimingMode,
    start: Instant,
    query_us: u64,
    phase: &str,
    phase_us: u64,
) -> Option<crate::execution::TimingBreakdown> {
    if timing_mode == crate::execution::TimingMode::Off {
        return None;
    }
    let total_us = start.elapsed().as_micros() as u64;
    Some(crate::execution::TimingBreakdown {
        total_us,
        parse_us: 0,
        sip_us: 0,
        magic_sets_us: 0,
        ir_build_us: 0,
        optimize_us: 0,
        shared_views_us: 0,
        rules: vec![
            crate::execution::timing::RuleTiming {
                rule_head: "query_execution".into(),
                execution_us: query_us,
                is_recursive: false,
                workers: 1,
            },
            crate::execution::timing::RuleTiming {
                rule_head: phase.into(),
                execution_us: phase_us,
                is_recursive: false,
                workers: 1,
            },
        ],
        optimizer_detail: None,
        ir_builder_detail: None,
    })
}

impl Handler {
    /// Create a new handler with the given storage engine.
    pub fn new(storage: StorageEngine) -> Self {
        let notifications = Arc::new(NotificationLog::new(
            storage.config().http.rate_limit.notification_buffer_size,
        ));
        let config = Arc::new(storage.config().clone());
        let ncpu = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
        // Reserve ~25% of cores (min 2) for Tokio async I/O, health checks, WebSocket handling.
        // The rest are available for CPU-bound DD computations via spawn_blocking.
        let io_reserve = (ncpu / 4).max(2).min(ncpu - 1);
        let compute_permits = ncpu - io_reserve;
        let total_query_memory = config.storage.performance.total_query_memory_bytes();
        Self {
            storage: Arc::new(RwLock::new(storage)),
            config,
            start_time: Instant::now(),
            query_count: Arc::new(AtomicU64::new(0)),
            insert_count: Arc::new(AtomicU64::new(0)),
            sessions: SessionManager::default(),
            notifications,
            query_semaphore: Arc::new(tokio::sync::Semaphore::new(compute_permits)),
            compute_permits,
            probe_semaphore: Arc::new(tokio::sync::Semaphore::new(PROBE_PERMITS)),
            query_memory: QueryMemoryPool::new(total_query_memory),
            timing_histograms: Arc::new(crate::execution::timing::TimingHistograms::new()),
            subscription_metrics: Arc::default(),
            subscription_hub: std::sync::OnceLock::new(),
            login_throttle: Arc::default(),
            password_permits: Arc::new(tokio::sync::Semaphore::new((ncpu / 4).clamp(1, 4))),
            login_queue: Arc::new(tokio::sync::Semaphore::new(MAX_QUEUED_LOGINS)),
            credentials: crate::auth::CredentialRegistry::default(),
            credential_writes: parking_lot::Mutex::new(()),
            kg_acl_generation: AtomicU64::new(0),
        }
    }

    /// Create a new handler from configuration.
    pub fn from_config(mut config: Config) -> Result<Self, String> {
        config.validate()?;
        let storage =
            StorageEngine::new(config).map_err(|e| format!("Failed to create storage: {e}"))?;
        let handler = Self::new(storage);
        info!(
            query_memory_bytes = handler.config.storage.performance.max_query_memory_bytes,
            total_query_memory_bytes = handler.query_memory.budget(),
            compute_permits = handler.query_semaphore.available_permits(),
            "query_memory_limits"
        );
        Ok(handler)
    }

    /// Create a new handler with custom session configuration.
    pub fn with_session_config(storage: StorageEngine, session_config: SessionConfig) -> Self {
        let notifications = Arc::new(NotificationLog::new(
            storage.config().http.rate_limit.notification_buffer_size,
        ));
        let config = Arc::new(storage.config().clone());
        let ncpu = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
        // Reserve ~25% of cores (min 2) for Tokio async I/O, health checks, WebSocket handling.
        let io_reserve = (ncpu / 4).max(2).min(ncpu - 1);
        let compute_permits = ncpu - io_reserve;
        let total_query_memory = config.storage.performance.total_query_memory_bytes();
        Self {
            storage: Arc::new(RwLock::new(storage)),
            config,
            start_time: Instant::now(),
            query_count: Arc::new(AtomicU64::new(0)),
            insert_count: Arc::new(AtomicU64::new(0)),
            sessions: SessionManager::new(session_config),
            notifications,
            query_semaphore: Arc::new(tokio::sync::Semaphore::new(compute_permits)),
            compute_permits,
            probe_semaphore: Arc::new(tokio::sync::Semaphore::new(PROBE_PERMITS)),
            query_memory: QueryMemoryPool::new(total_query_memory),
            timing_histograms: Arc::new(crate::execution::timing::TimingHistograms::new()),
            subscription_metrics: Arc::default(),
            subscription_hub: std::sync::OnceLock::new(),
            login_throttle: Arc::default(),
            password_permits: Arc::new(tokio::sync::Semaphore::new((ncpu / 4).clamp(1, 4))),
            login_queue: Arc::new(tokio::sync::Semaphore::new(MAX_QUEUED_LOGINS)),
            credentials: crate::auth::CredentialRegistry::default(),
            credential_writes: parking_lot::Mutex::new(()),
            kg_acl_generation: AtomicU64::new(0),
        }
    }

    /// Create a QueryJob with Arc-cloned fields for use in `spawn_blocking`.
    fn make_query_job(&self) -> QueryJob {
        QueryJob {
            storage: Arc::clone(&self.storage),
            config: Arc::clone(&self.config),
            notifications: Arc::clone(&self.notifications),
            insert_count: Arc::clone(&self.insert_count),
            query_count: Arc::clone(&self.query_count),
            start_time: self.start_time,
            timing_histograms: Arc::clone(&self.timing_histograms),
            pinned: None,
            cache_plan: None,
        }
    }

    /// The control of a request arriving now: its deadline is
    /// `query_timeout_ms` from now, or the client's `timeout_ms` when that is
    /// sooner (no deadline when both are unset or the server's is 0).
    pub fn request_control(&self, timeout_ms: Option<u64>) -> Arc<RequestControl> {
        let server_ms = match self.config.storage.performance.query_timeout_ms {
            0 => None,
            ms => Some(ms),
        };
        let ms = match (timeout_ms, server_ms) {
            (Some(client), Some(server)) => Some(client.min(server)),
            (client, server) => client.or(server),
        };
        RequestControl::limited(
            // A deadline past what `Instant` can represent is no deadline.
            ms.and_then(|ms| Instant::now().checked_add(std::time::Duration::from_millis(ms))),
            self.config.storage.performance.max_query_memory_bytes,
            Some(Arc::clone(&self.query_memory)),
        )
    }

    /// Get reference to the handler's configuration.
    pub fn config(&self) -> &crate::Config {
        &self.config
    }

    /// How many queries run at once; more wait for a compute permit.
    pub fn compute_permits(&self) -> usize {
        self.compute_permits
    }

    /// Every compute permit, held until the result drops.
    #[cfg(test)]
    pub(crate) fn hold_compute_permits(&self) -> tokio::sync::OwnedSemaphorePermit {
        Arc::clone(&self.query_semaphore)
            .try_acquire_many_owned(self.compute_permits as u32)
            .unwrap_or_else(|e| panic!("compute permits taken: {e}"))
    }

    /// This handler running `permits` queries at once (by default, one per
    /// core not reserved for I/O).
    pub fn with_compute_permits(mut self, permits: usize) -> Self {
        self.query_semaphore = Arc::new(tokio::sync::Semaphore::new(permits));
        self.compute_permits = permits;
        self
    }

    /// Standing-query counters.
    pub fn subscription_metrics(&self) -> &super::subscription::SubscriptionMetrics {
        &self.subscription_metrics
    }

    /// The worker owning the shared standing-query views, started on the
    /// current runtime at the first call.
    pub fn subscription_hub(&self) -> &super::subscription::SubscriptionHub {
        self.subscription_hub.get_or_init(|| {
            super::subscription::SubscriptionHub::spawn(
                self.notifications.subscribe(),
                Arc::clone(&self.subscription_metrics),
                std::time::Duration::from_millis(
                    self.config.http.rate_limit.subscription_coalesce_ms,
                ),
            )
        })
    }

    /// Live change notifications from now on.
    pub fn subscribe_notifications(&self) -> tokio::sync::broadcast::Receiver<Notification> {
        self.notifications.subscribe()
    }

    /// The ordered change-notification stream of this engine run.
    pub fn notifications(&self) -> &NotificationLog {
        &self.notifications
    }

    fn send_notification(&self, notification: Notification) {
        self.notifications.publish(notification);
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
        self.send_notification(Notification::PersistentUpdate {
            knowledge_graph: kg.to_string(),
            relation: relation.to_string(),
            operation: operation.to_string(),
            count,
            timestamp_ms: now_ms(),
            session_id: None,
            seq: 0,
        });
    }

    /// Send a rule change notification.
    pub fn notify_rule_change(&self, kg: &str, rule_name: &str, operation: &str) {
        self.send_notification(Notification::RuleChange {
            knowledge_graph: kg.to_string(),
            rule_name: rule_name.to_string(),
            operation: operation.to_string(),
            timestamp_ms: now_ms(),
            seq: 0,
        });
    }

    /// Send a knowledge graph change notification.
    pub fn notify_kg_change(&self, kg: &str, operation: &str) {
        self.send_notification(Notification::KgChange {
            knowledge_graph: kg.to_string(),
            operation: operation.to_string(),
            timestamp_ms: now_ms(),
            seq: 0,
        });
    }

    /// Send a schema change notification.
    pub fn notify_schema_change(&self, kg: &str, entity: &str, operation: &str) {
        self.send_notification(Notification::SchemaChange {
            knowledge_graph: kg.to_string(),
            entity: entity.to_string(),
            operation: operation.to_string(),
            timestamp_ms: now_ms(),
            seq: 0,
        });
    }

    /// Get the session manager.
    pub fn session_manager(&self) -> &SessionManager {
        &self.sessions
    }

    /// Create a new session bound to a knowledge graph.
    pub fn create_session(&self, knowledge_graph: &str) -> Result<SessionId, String> {
        // Block direct access to the system KG
        if knowledge_graph == crate::auth::INTERNAL_KG {
            return Err(format!(
                "Access denied: '{}' is a system knowledge graph",
                crate::auth::INTERNAL_KG
            ));
        }

        // Validate KG exists (or auto-create if configured)
        let storage = self.storage.read();
        storage
            .ensure_knowledge_graph(knowledge_graph)
            .map_err(|e| format!("Knowledge graph '{knowledge_graph}' not found: {e}"))?;
        drop(storage);
        self.sessions.create_session(knowledge_graph)
    }

    /// Create a session with per-KG access check.
    /// Rejects if the user has no access to the requested KG.
    pub fn create_session_with_auth(
        &self,
        knowledge_graph: &str,
        principal: &crate::auth::Principal,
    ) -> Result<SessionId, String> {
        let auth = principal.identity()?;
        // Admins skip per-KG checks
        if auth.role != crate::auth::Role::Admin && self.kg_access(knowledge_graph, &auth).is_none()
        {
            return Err("Access denied".to_string());
        }
        self.create_session(knowledge_graph)
    }

    /// Close a session.
    pub fn close_session(&self, session_id: &SessionId) -> Result<(), String> {
        self.sessions.close_session(session_id)
    }

    /// Get uptime in seconds.
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Get access to the storage engine (for HTTP handlers).
    pub fn get_storage(&self) -> parking_lot::RwLockReadGuard<'_, StorageEngine> {
        self.storage.read()
    }

    /// Try to acquire a read lock on the storage engine with a timeout.
    /// Returns `None` if the lock cannot be acquired within the given duration.
    /// Used by the health check to detect a degraded state (e.g. lock convoy).
    pub fn try_get_storage(
        &self,
        timeout: std::time::Duration,
    ) -> Option<parking_lot::RwLockReadGuard<'_, StorageEngine>> {
        self.storage.try_read_for(timeout)
    }

    /// Graceful shutdown: flush WAL and save metadata for all knowledge graphs.
    pub fn shutdown(&self) {
        self.persist_api_key_usage();
        info!("Flushing WAL and saving metadata...");
        if let Err(e) = self.storage.read().save_all() {
            warn!(error = %e, "Error during shutdown save");
        }
        info!("Shutdown complete.");
    }

    // ── Auth / RBAC ─────────────────────────────────────────────────────────

    /// Bootstrap auth: create the `_internal` knowledge graph and an admin
    /// user if there is none, then load the credential registry from it.
    /// Called once on server startup; until then no credential authenticates.
    pub fn bootstrap_auth(&self) {
        self.seed_admin_credentials();
        self.load_credentials();
    }

    /// Load every user and API key from `_internal` into the registry.
    fn load_credentials(&self) {
        use crate::auth;

        let snapshot = self.storage.read().get_snapshot_for(auth::INTERNAL_KG);
        match snapshot {
            Ok(snapshot) => {
                let (users, keys) = auth::stored_credentials(&snapshot.input_tuples);
                self.credentials.load(users, keys);
            }
            Err(e) => warn!(error = %e, "auth_credentials_load_failed"),
        }
    }

    /// Insert the bootstrap admin user when `_internal` has no users, with an
    /// API key only if bootstrap has never issued one for this data directory.
    /// The key is saved to the credentials file and printed only once stored.
    fn seed_admin_credentials(&self) {
        use crate::auth;

        let Some(issue_api_key) = self.bootstrap_needed() else {
            return;
        };

        let credentials_path = self
            .config
            .http
            .auth
            .credentials_file
            .clone()
            .unwrap_or_else(|| self.config.storage.data_dir.join("credentials.toml"));
        let persisted = auth::PersistedCredentials::load(&credentials_path).unwrap_or_default();

        // Precedence: env var / config > persisted file > generated. Supplied
        // secrets are never written to disk.
        let supplied_password = std::env::var("INPUTLAYER_ADMIN_PASSWORD")
            .ok()
            .or_else(|| self.config.http.auth.bootstrap_admin_password.clone());
        let supplied_api_key = std::env::var("INPUTLAYER_BOOTSTRAP_API_KEY")
            .ok()
            .filter(|k| !k.is_empty());
        let mut to_persist = auth::PersistedCredentials::default();
        let resolve =
            |supplied: Option<String>, persisted: Option<String>, slot: &mut Option<String>| {
                if let Some(value) = supplied {
                    return (value, false);
                }
                let generated = persisted.is_none();
                let value = persisted.unwrap_or_else(auth::generate_api_key);
                *slot = Some(value.clone());
                (value, generated)
            };
        let (password, password_generated) = resolve(
            supplied_password,
            persisted.admin_password,
            &mut to_persist.admin_password,
        );
        let (mut api_key, key_generated) = if issue_api_key {
            let (key, generated) =
                resolve(supplied_api_key, persisted.api_key, &mut to_persist.api_key);
            (Some(key), generated)
        } else {
            (None, false)
        };

        let hash = match auth::hash_password(&password) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("ERROR: Failed to hash admin password: {e}");
                return;
            }
        };

        // The password is saved before the user exists; the key only once
        // stored.
        if password_generated {
            let password_only = auth::PersistedCredentials {
                admin_password: to_persist.admin_password.clone(),
                api_key: None,
            };
            if let Err(e) = password_only.save(&credentials_path) {
                warn!(
                    error = %e,
                    path = %credentials_path.display(),
                    "Failed to save credentials file"
                );
                eprintln!(
                    "ERROR: cannot save generated credentials to {}: {e}. Admin user not created.",
                    credentials_path.display()
                );
                return;
            }
        } else if to_persist != auth::PersistedCredentials::default() {
            info!(
                "Auth bootstrap: reusing credentials from {}",
                credentials_path.display()
            );
        }

        let _credential_writes = self.credential_writes.lock();
        let storage = self.storage.read();
        if let Err(e) = self.create_user(
            &storage,
            auth::UserRecord {
                username: "admin".to_string(),
                password_hash: hash,
                role: auth::Role::Admin,
            },
        ) {
            warn!(error = %e, "Failed to insert admin user");
            return;
        }
        info!("Auth bootstrap: admin user created");

        if let Some(key) = &api_key {
            if let Err(e) = store_bootstrap_key(&storage, key) {
                warn!(error = %e, "Failed to insert bootstrap API key");
                api_key = None;
            } else {
                info!("Auth bootstrap: API key 'bootstrap' created for admin");
            }
        }

        if api_key.is_some() && (password_generated || key_generated) {
            if let Err(e) = to_persist.save(&credentials_path) {
                warn!(
                    error = %e,
                    path = %credentials_path.display(),
                    "Failed to save credentials file"
                );
                eprintln!(
                    "WARNING: cannot save the bootstrap API key to {}: {e}.",
                    credentials_path.display()
                );
                api_key = None;
            }
        }

        if password_generated || (key_generated && api_key.is_some()) {
            info!(
                "Auth bootstrap: credentials saved to {}",
                credentials_path.display()
            );
            eprintln!();
            eprintln!("=== INITIAL ADMIN CREDENTIALS CREATED ===");
            if let Some(api_key) = &api_key {
                // Masked to keep the key out of logs.
                let masked = match api_key.len() {
                    n if n > 4 => api_key.get(n - 4..).unwrap_or(""),
                    _ => "",
                };
                eprintln!("Admin API key: ****{masked}");
            }
            eprintln!("==========================================");
            eprintln!();
            eprintln!(
                "Generated credentials saved to: {}",
                credentials_path.display()
            );
            eprintln!("Retrieve them with:  cat {}", credentials_path.display());
            eprintln!("Delete this file to generate new credentials on next boot.");
            eprintln!();
        }
    }

    /// Prepare `_internal` for bootstrap and backfill the record of an already
    /// issued bootstrap key. `None` when admin bootstrap has nothing to do;
    /// otherwise whether it should issue an API key.
    fn bootstrap_needed(&self) -> Option<bool> {
        use crate::auth;

        let _credential_writes = self.credential_writes.lock();
        let storage = self.storage.read();

        // Create _internal KG if it doesn't exist
        if storage.ensure_knowledge_graph(auth::INTERNAL_KG).is_err() {
            // Try explicit create if ensure fails (auto_create might be off)
            if let Err(e) = storage.create_knowledge_graph(auth::INTERNAL_KG) {
                // Already exists is fine
                if !format!("{e}").contains("already exists") {
                    warn!(error = %e, "Failed to create _internal KG");
                    return None;
                }
            }
        }

        // Check if users relation already has data
        let snapshot = match storage.get_snapshot_for(auth::INTERNAL_KG) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "Failed to get _internal snapshot");
                return None;
            }
        };

        let has_users = snapshot
            .input_tuples
            .get("users")
            .is_some_and(|t| !t.is_empty());
        let key_recorded = snapshot
            .input_tuples
            .get(auth::stored::BOOTSTRAP_KEYS)
            .is_some_and(|t| !t.is_empty());
        let bootstrapped_before = has_users
            || api_keys::read_api_keys(&snapshot)
                .iter()
                .any(|key| key.label == BOOTSTRAP_KEY_LABEL);
        if !key_recorded && bootstrapped_before {
            if let Err(e) = record_bootstrap_key(&storage) {
                warn!(error = %e, "Failed to record bootstrap API key");
            }
        }

        if has_users {
            info!("Auth bootstrap: admin user already exists");
            return None;
        }
        Some(!key_recorded && !bootstrapped_before)
    }

    /// Authenticate a user by username and password. Synchronous and
    /// CPU-heavy (argon2); servers should call [`Handler::login`].
    pub fn authenticate_user(
        &self,
        username: &str,
        password: &str,
    ) -> Result<crate::auth::Principal, String> {
        let candidate = self.credentials.password_candidate(username);
        let hash = candidate.as_ref().map(|c| c.password_hash.as_str());
        if !crate::auth::verify_password_or_dummy(password, hash) {
            return Err("Invalid credentials".to_string());
        }
        // Verified but replaced meanwhile: the old password is no longer valid.
        candidate
            .ok_or_else(|| "Invalid credentials".to_string())?
            .accept()
            .map_err(|_| "Invalid credentials".to_string())
    }

    /// Password login from `peer`: throttled per IP and username, with
    /// argon2 on the blocking pool behind a small semaphore. The outcome is
    /// recorded even if the caller stops waiting.
    pub async fn login(
        self: &Arc<Self>,
        username: &str,
        password: &str,
        peer: std::net::IpAddr,
    ) -> Result<crate::auth::Principal, String> {
        let attempt = self.login_throttle.begin(peer, username).map_err(|wait| {
            let retry_secs = wait.as_secs().max(1);
            warn!(username, %peer, retry_secs, "audit_auth_login_throttled");
            format!("Too many failed login attempts; retry in {retry_secs}s")
        })?;
        let Ok(queued) = Arc::clone(&self.login_queue).try_acquire_owned() else {
            self.login_throttle.abort(&attempt);
            warn!(username, %peer, "audit_auth_login_busy");
            return Err("Authentication service busy; retry later".to_string());
        };
        let unavailable = || "Authentication service unavailable".to_string();
        let handler = Arc::clone(self);
        let (user, pass) = (username.to_string(), password.to_string());
        tokio::spawn(async move {
            let _queued = queued;
            let result = match Arc::clone(&handler.password_permits).acquire_owned().await {
                Ok(permit) => {
                    let verifier = Arc::clone(&handler);
                    let user = user.clone();
                    tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        verifier.authenticate_user(&user, &pass)
                    })
                    .await
                    .unwrap_or_else(|_| Err(unavailable()))
                }
                Err(_) => Err(unavailable()),
            };
            match &result {
                Ok(principal) => {
                    handler.login_throttle.succeed(&attempt);
                    info!(username = %user, credential = %principal.credential(), %peer, "audit_auth_login_success");
                }
                Err(_) => {
                    handler.login_throttle.fail(&attempt);
                    warn!(username = %user, %peer, "audit_auth_login_failed");
                }
            }
            result
        })
        .await
        .unwrap_or_else(|_| Err(unavailable()))
    }

    /// Authenticate an API key: one hash and one registry lookup.
    pub fn authenticate_api_key(&self, key: &str) -> Result<crate::auth::Principal, String> {
        match self
            .credentials
            .authenticate_key(&crate::auth::hash_api_key(key))
        {
            Ok(principal) => {
                tracing::info!(
                    username = principal.username(),
                    credential = %principal.credential(),
                    "audit_auth_apikey_success"
                );
                Ok(principal)
            }
            Err(rejected) => {
                tracing::warn!(reason = %rejected, "audit_auth_apikey_rejected");
                Err(rejected.to_string())
            }
        }
    }

    // ── User CRUD ───────────────────────────────────────────────────────────

    /// List all users (returns username and role, never the hash).
    pub fn handle_user_list(&self) -> Result<QueryResult, ProgramError> {
        use crate::auth;

        let storage = self.storage.read();
        let snapshot = storage
            .get_snapshot_for(auth::INTERNAL_KG)
            .map_err(ProgramError::from)?;
        drop(storage);

        let empty_vec = crate::value::Relation::new();
        let users = snapshot.input_tuples.get("users").unwrap_or(&empty_vec);

        let mut rows = Vec::new();
        for tuple in users {
            let vals = tuple.values();
            if vals.len() >= 3 {
                rows.push(WireTuple {
                    values: vec![
                        WireValue::from_value(&vals[0]), // username
                        WireValue::from_value(&vals[2]), // role
                    ],
                    provenance: None,
                });
            }
        }

        let total_count = rows.len();
        Ok(QueryResult {
            rows,
            schema: vec![
                ColumnDef {
                    name: "username".to_string(),
                    data_type: WireDataType::String,
                },
                ColumnDef {
                    name: "role".to_string(),
                    data_type: WireDataType::String,
                },
            ],
            total_count,
            truncated: false,
            execution_time_ms: 0,
            metadata: None,
            switched_kg: None,
            proof_trees: None,
            timing_breakdown: None,
            errors: Vec::new(),
            statements: Vec::new(),
            revision: None,
        })
    }

    /// Create a new user.
    pub fn handle_user_create(
        &self,
        username: &str,
        password: &str,
        role_str: &str,
    ) -> Result<QueryResult, ProgramError> {
        use crate::auth;
        use std::str::FromStr;

        let role = auth::Role::from_str(role_str)?;
        let password_hash = auth::hash_password(password)?;

        let _credential_writes = self.credential_writes.lock();
        let storage = self.storage.read();
        self.create_user(
            &storage,
            auth::UserRecord {
                username: username.to_string(),
                password_hash,
                role,
            },
        )?;

        tracing::info!(username, role = role_str, "audit_user_created");
        Ok(self.message_result(&format!(
            "User '{username}' created with role '{role_str}'."
        )))
    }

    fn create_user(
        &self,
        storage: &StorageEngine,
        user: crate::auth::UserRecord,
    ) -> Result<(), ProgramError> {
        use crate::auth;

        let username = &user.username;
        let snapshot = storage
            .get_snapshot_for(auth::INTERNAL_KG)
            .map_err(ProgramError::from)?;

        if user_exists(&snapshot, username) {
            return Err(format!("User '{username}' already exists").into());
        }

        self.credentials.remove_user(username);
        let tuple = Tuple::new(vec![
            Value::string(username),
            Value::string(&user.password_hash),
            Value::string(&user.role.to_string()),
        ]);
        storage
            .insert_tuples_into(auth::INTERNAL_KG, "users", vec![tuple])
            .map_err(ProgramError::from)?;
        self.credentials.put_user(user);
        Ok(())
    }

    /// Delete everything `username` holds besides its user row: its API keys
    /// (revoking them), their times and its KG grants, so a recreated username
    /// never inherits them. Keys and grants are separate writes: a transient failure of one does not
    /// stop the other (strip as much access as possible), and its error is
    /// returned at the end; an unknown outcome or a read-only store returns at
    /// once.
    fn delete_user_access(
        &self,
        storage: &StorageEngine,
        username: &str,
    ) -> Result<(), ProgramError> {
        use crate::auth;
        use crate::storage::StorageError;

        let snapshot = storage
            .get_snapshot_for(auth::INTERNAL_KG)
            .map_err(ProgramError::from)?;
        let mut deferred = None;
        // An unknown outcome or a read-only store stops at once; any other
        // failure is reported after the remaining cleanup ran.
        let mut settle = |result: Result<(), StorageError>| match result {
            Err(error @ (StorageError::OutcomeUnknown { .. } | StorageError::StoreReadOnly)) => {
                Err(ProgramError::from(error))
            }
            Err(error) => {
                deferred.get_or_insert(error);
                Ok(())
            }
            Ok(()) => Ok(()),
        };

        settle(
            api_keys::delete_api_keys(storage, &snapshot, |key| key.username == username).map(
                |labels| {
                    for label in labels {
                        self.credentials.revoke_key(&label);
                    }
                },
            ),
        )?;
        let grants = acl_grant_deletes(&snapshot, |_, user| user == username);
        if !grants.is_empty() {
            let deleted = commit_internal(storage, grants);
            self.kg_acls_changed();
            settle(deleted)?;
        }
        deferred.map_or(Ok(()), |error| Err(error.into()))
    }

    /// Drop a user.
    pub fn handle_user_drop(&self, username: &str) -> Result<QueryResult, ProgramError> {
        use crate::auth;

        if username == "admin" {
            return Err("Cannot drop the 'admin' user".to_string().into());
        }

        let _credential_writes = self.credential_writes.lock();
        let storage = self.storage.read();
        let snapshot = storage
            .get_snapshot_for(auth::INTERNAL_KG)
            .map_err(ProgramError::from)?;

        // Find user tuple to delete
        let empty_vec = crate::value::Relation::new();
        let users = snapshot.input_tuples.get("users").unwrap_or(&empty_vec);
        let mut found = None;
        for tuple in users {
            if let Some(u) = tuple.values().first().and_then(|v| v.as_str()) {
                if u == username {
                    found = Some(tuple.clone());
                    break;
                }
            }
        }

        let tuple = found.ok_or_else(|| format!("User '{username}' not found"))?;

        // Keys with their times, and KG grants, go first as separate
        // best-effort writes; the user row only once both succeed, so a
        // failure leaves the user intact.
        self.delete_user_access(&storage, username)?;
        self.credentials.remove_user(username);
        storage.delete_tuples_from(auth::INTERNAL_KG, "users", vec![tuple])?;

        tracing::info!(username, "audit_user_dropped");
        Ok(self.message_result(&format!("User '{username}' dropped.")))
    }

    /// Change a user's password.
    pub fn handle_user_password(
        &self,
        username: &str,
        new_password: &str,
    ) -> Result<QueryResult, ProgramError> {
        use crate::auth;
        use crate::value::Value;

        let new_hash = auth::hash_password(new_password)?;
        let _credential_writes = self.credential_writes.lock();
        let storage = self.storage.read();
        let snapshot = storage
            .get_snapshot_for(auth::INTERNAL_KG)
            .map_err(ProgramError::from)?;

        // Find existing user
        let empty_vec = crate::value::Relation::new();
        let users = snapshot.input_tuples.get("users").unwrap_or(&empty_vec);
        let mut old_tuple = None;
        let mut role_str = String::new();
        for tuple in users {
            let vals = tuple.values();
            if vals.len() >= 3 {
                if let Some(u) = vals[0].as_str() {
                    if u == username {
                        role_str = vals[2].as_str().unwrap_or("viewer").to_string();
                        old_tuple = Some(tuple.clone());
                        break;
                    }
                }
            }
        }

        let old = old_tuple.ok_or_else(|| format!("User '{username}' not found"))?;
        let new_tuple = crate::value::Tuple::new(vec![
            Value::string(username),
            Value::string(&new_hash),
            Value::string(&role_str),
        ]);

        self.replace_user_row(&storage, username, old, new_tuple)?;
        self.credentials.set_password(username, new_hash);

        tracing::info!(username, "audit_user_password_changed");
        Ok(self.message_result(&format!("Password updated for '{username}'.")))
    }

    /// Replace `old` with `new` in `users` as one program, so a failure
    /// leaves the old row. A durable write whose outcome is unknown revokes
    /// the user's live credentials, which may no longer match storage.
    fn replace_user_row(
        &self,
        storage: &StorageEngine,
        username: &str,
        old: Tuple,
        new: Tuple,
    ) -> Result<(), ProgramError> {
        use crate::storage_engine::{CommitError, FactChange, StagedChanges, WriteProgram};

        let program = WriteProgram::single(StagedChanges::Facts(vec![
            FactChange::Delete {
                relation: "users".to_string(),
                tuples: vec![old],
            },
            FactChange::Insert {
                relation: "users".to_string(),
                tuples: vec![new],
            },
        ]));
        storage
            .commit_program(crate::auth::INTERNAL_KG, program, None)
            .map(drop)
            .map_err(|e| {
                // Durable but possibly unapplied, or possibly recovered on
                // restart: fail closed until restart settles it.
                if matches!(e, CommitError::Unknown(_) | CommitError::OutcomeUnknown(_)) {
                    self.credentials.remove_user(username);
                }
                ProgramError::from(e.into_storage_error())
            })
    }

    /// Change a user's role.
    pub fn handle_user_role(
        &self,
        username: &str,
        new_role: &str,
    ) -> Result<QueryResult, ProgramError> {
        use crate::auth;
        use crate::value::Value;
        use std::str::FromStr;

        let role = auth::Role::from_str(new_role)?;

        if username == "admin" && new_role != "admin" {
            return Err("Cannot change the 'admin' user's role".to_string().into());
        }

        let _credential_writes = self.credential_writes.lock();
        let storage = self.storage.read();
        let snapshot = storage
            .get_snapshot_for(auth::INTERNAL_KG)
            .map_err(ProgramError::from)?;

        // Find existing user
        let empty_vec = crate::value::Relation::new();
        let users = snapshot.input_tuples.get("users").unwrap_or(&empty_vec);
        let mut old_tuple = None;
        let mut hash_str = String::new();
        for tuple in users {
            let vals = tuple.values();
            if vals.len() >= 3 {
                if let Some(u) = vals[0].as_str() {
                    if u == username {
                        hash_str = vals[1].as_str().unwrap_or("").to_string();
                        old_tuple = Some(tuple.clone());
                        break;
                    }
                }
            }
        }

        let old = old_tuple.ok_or_else(|| format!("User '{username}' not found"))?;
        let new_tuple = crate::value::Tuple::new(vec![
            Value::string(username),
            Value::string(&hash_str),
            Value::string(new_role),
        ]);

        self.replace_user_row(&storage, username, old, new_tuple)?;
        self.credentials.set_role(username, role);

        Ok(self.message_result(&format!("Role updated to '{new_role}' for '{username}'.")))
    }

    // ── KG ACL management ─────────────────────────────────────────────────

    /// Get a user's effective access to a specific knowledge graph.
    /// Admins are implicitly owners of all KGs.
    /// Returns None if the user has no access.
    pub fn get_kg_role_for_user(
        &self,
        kg_name: &str,
        username: &str,
        global_role: &crate::auth::Role,
    ) -> Option<crate::auth::KgAccess> {
        use crate::auth;

        // Admins are implicit owners of all KGs
        if *global_role == auth::Role::Admin {
            return Some(auth::KgRole::Owner.into());
        }

        let storage = self.storage.read();
        let snapshot = match storage.get_snapshot_for(auth::INTERNAL_KG) {
            Ok(s) => s,
            Err(_) => return None,
        };

        let empty_vec = crate::value::Relation::new();
        let acls = snapshot.input_tuples.get("kg_acls").unwrap_or(&empty_vec);

        // Find matching ACL: kg_acls(kg_name, username, role)
        for tuple in acls {
            let vals = tuple.values();
            if vals.len() >= 3 {
                if let (Some(kg), Some(user), Some(role)) =
                    (vals[0].as_str(), vals[1].as_str(), vals[2].as_str())
                {
                    if kg == kg_name && user == username {
                        let role = role.parse::<auth::KgRole>().ok()?;
                        return kg_access_from_acl(&snapshot, kg_name, username, role);
                    }
                }
            }
        }

        None
    }

    /// The access `identity` has to `kg_name` right now: its user's, narrowed
    /// to its key's scope when it has one. A scoped key has no access to any
    /// other KG.
    pub fn kg_access(
        &self,
        kg_name: &str,
        identity: &crate::auth::AuthIdentity,
    ) -> Option<crate::auth::KgAccess> {
        let Some(key) = &identity.key_scope else {
            return self.get_kg_role_for_user(kg_name, &identity.username, &identity.role);
        };
        if key.scope.kg != kg_name {
            return None;
        }
        self.get_kg_role_for_user(kg_name, &identity.username, &key.owner_role)
            .map(|own| own.meet(&key.scope.access))
    }

    /// List ACL entries for a knowledge graph.
    pub fn handle_kg_acl_list(&self, kg_name: &str) -> Result<String, ProgramError> {
        use crate::auth;

        let storage = self.storage.read();
        let snapshot = storage
            .get_snapshot_for(auth::INTERNAL_KG)
            .map_err(ProgramError::from)?;

        let empty_vec = crate::value::Relation::new();
        let acls = snapshot.input_tuples.get("kg_acls").unwrap_or(&empty_vec);

        let mut entries = Vec::new();
        for tuple in acls {
            let vals = tuple.values();
            if vals.len() >= 3 {
                if let (Some(kg), Some(user), Some(role)) =
                    (vals[0].as_str(), vals[1].as_str(), vals[2].as_str())
                {
                    if kg == kg_name {
                        let access = role
                            .parse()
                            .ok()
                            .and_then(|role| kg_access_from_acl(&snapshot, kg, user, role));
                        match access {
                            Some(access) => entries.push(format!("  {user}: {access}")),
                            None => entries.push(format!("  {user}: {role} (unreadable grant)")),
                        }
                    }
                }
            }
        }

        if entries.is_empty() {
            Ok(format!(
                "No ACL entries for '{kg_name}'. Admins have implicit owner access."
            ))
        } else {
            Ok(format!(
                "ACL entries for '{kg_name}':\n{}",
                entries.join("\n")
            ))
        }
    }

    /// Grant a user access to a knowledge graph.
    pub fn handle_kg_acl_grant(
        &self,
        kg_name: &str,
        username: &str,
        role: &str,
    ) -> Result<String, ProgramError> {
        self.grant_kg_access(kg_name, username, role, None)
    }

    /// Grant a user `role` on a knowledge graph, limited to `relations` for a
    /// `writer` or `decider`. Replaces the user's previous grant on it in one
    /// commit, so no reader ever sees the new role with the old relations.
    pub fn grant_kg_access(
        &self,
        kg_name: &str,
        username: &str,
        role: &str,
        relations: Option<&[String]>,
    ) -> Result<String, ProgramError> {
        use crate::auth;
        use crate::storage_engine::FactChange;

        let kg_role: auth::KgRole = role.parse().map_err(|_| {
            format!("Invalid KG role '{role}'. Valid: owner, editor, writer, decider, viewer")
        })?;
        let access = auth::KgAccess::new(kg_role, relations.map(<[String]>::to_vec))?;

        let _credential_writes = self.credential_writes.lock();
        let storage = self.storage.read();

        // Verify KG exists
        storage
            .get_snapshot_for(kg_name)
            .map_err(|_| format!("Knowledge graph '{kg_name}' not found"))?;

        let snapshot = storage
            .get_snapshot_for(auth::INTERNAL_KG)
            .map_err(ProgramError::from)?;
        if !user_exists(&snapshot, username) {
            return Err(format!("User '{username}' not found").into());
        }

        // Replace any existing grant for this user+kg.
        let mut changes =
            acl_grant_deletes(&snapshot, |kg, user| kg == kg_name && user == username);
        if let Some(granted) = access.relations() {
            changes.push(FactChange::Insert {
                relation: KG_ACL_RELATIONS.to_string(),
                tuples: vec![Tuple::new(vec![
                    Value::string(kg_name),
                    Value::string(username),
                    Value::string(&auth::encode_relations(Some(granted))),
                ])],
            });
        }
        changes.push(FactChange::Insert {
            relation: "kg_acls".to_string(),
            tuples: vec![Tuple::new(vec![
                Value::string(kg_name),
                Value::string(username),
                Value::string(&kg_role.to_string()),
            ])],
        });
        let granted = commit_internal(&storage, changes);
        self.kg_acls_changed();
        granted?;

        tracing::info!(kg = kg_name, user = username, access = %access, "audit_kg_acl_granted");
        Ok(format!(
            "Granted '{access}' access on '{kg_name}' to '{username}'."
        ))
    }

    /// Count a change to the KG access lists, after it is visible.
    fn kg_acls_changed(&self) {
        self.kg_acl_generation.fetch_add(1, Ordering::Release);
    }

    /// Changes so far to the KG access lists: a reader that saw a value and
    /// sees it again knows no grant or revocation happened in between.
    pub fn kg_acl_generation(&self) -> u64 {
        self.kg_acl_generation.load(Ordering::Acquire)
    }

    /// Revoke a user's access to a knowledge graph.
    pub fn handle_kg_acl_revoke(
        &self,
        kg_name: &str,
        username: &str,
    ) -> Result<String, ProgramError> {
        use crate::auth;

        let storage = self.storage.read();
        let snapshot = storage
            .get_snapshot_for(auth::INTERNAL_KG)
            .map_err(ProgramError::from)?;

        let changes = acl_grant_deletes(&snapshot, |kg, user| kg == kg_name && user == username);
        if !changes.iter().any(|change| change.relation() == "kg_acls") {
            return Err(format!("No ACL entry found for user '{username}' on '{kg_name}'").into());
        }

        let revoked = commit_internal(&storage, changes);
        self.kg_acls_changed();
        revoked?;

        tracing::info!(kg = kg_name, user = username, "audit_kg_acl_revoked");
        Ok(format!("Revoked access on '{kg_name}' from '{username}'."))
    }

    /// Remove all ACL entries for a dropped knowledge graph, and revoke the
    /// API keys scoped to it: a KG created later under the same name grants
    /// nothing from before.
    fn cleanup_kg_acls(&self, kg_name: &str) -> Result<(), ProgramError> {
        use crate::auth;

        let _credential_writes = self.credential_writes.lock();
        let storage = self.storage.read();
        let snapshot = storage.get_snapshot_for(auth::INTERNAL_KG)?;
        let scoped_here =
            |key: &auth::ApiKeyRecord| key.scope.as_ref().is_some_and(|scope| scope.kg == kg_name);
        for label in api_keys::delete_api_keys(&storage, &snapshot, scoped_here)? {
            self.credentials.revoke_key(&label);
            info!(label, kg = kg_name, "audit_apikey_revoked");
        }

        let changes = acl_grant_deletes(&snapshot, |kg, _| kg == kg_name);
        if !changes.is_empty() {
            let count: usize = changes
                .iter()
                .filter(|change| change.relation() == "kg_acls")
                .map(|change| match change {
                    crate::storage_engine::FactChange::Delete { tuples, .. } => tuples.len(),
                    crate::storage_engine::FactChange::Insert { .. } => 0,
                })
                .sum();
            let removed = commit_internal(&storage, changes);
            self.kg_acls_changed();
            removed?;
            tracing::info!(kg = kg_name, count, "audit_kg_acls_cleaned_up");
        }
        Ok(())
    }

    /// Get mutable access to the storage engine.
    ///
    /// **Warning**: This acquires a write lock on the entire storage engine.
    /// Only use in tests - production code should use `get_storage()` (read lock).
    pub fn get_storage_mut(&self) -> parking_lot::RwLockWriteGuard<'_, StorageEngine> {
        self.storage.write()
    }

    /// Get total queries executed.
    pub fn total_queries(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }

    /// Get reference to the accumulated timing histograms for Prometheus export.
    pub fn timing_histograms(&self) -> &crate::execution::timing::TimingHistograms {
        &self.timing_histograms
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

    /// Clear all facts from relations matching a prefix in a knowledge graph.
    /// Returns list of (relation_name, count_deleted) for each affected relation.
    pub fn clear_relations_by_prefix_in(
        &self,
        kg: &str,
        prefix: &str,
    ) -> Result<Vec<(String, usize)>, ProgramError> {
        let storage = self.storage.read();
        storage
            .clear_relations_by_prefix_in(kg, prefix)
            .map_err(ProgramError::from)
    }

    /// Drop all rules matching a prefix in a knowledge graph.
    pub fn drop_rules_by_prefix_in(
        &self,
        kg: &str,
        prefix: &str,
    ) -> Result<Vec<String>, ProgramError> {
        let storage = self.storage.read();
        storage
            .drop_rules_by_prefix_in(kg, prefix)
            .map_err(ProgramError::from)
    }

    // === Index Management API ===

    /// Create and build an HNSW index on a knowledge graph.
    pub fn create_index(
        &self,
        kg: &str,
        opts: &IndexCreateOptions,
    ) -> Result<String, ProgramError> {
        index_commands::create(&self.storage.read(), kg, opts)
    }

    /// Drop an index.
    pub fn drop_index(&self, kg: &str, name: &str) -> Result<String, ProgramError> {
        index_commands::drop(&self.storage.read(), kg, name)
    }

    /// List all indexes of a knowledge graph.
    pub fn list_indexes(&self, kg: &str) -> Result<Vec<IndexStats>, String> {
        index_commands::stats(&self.storage.read(), kg, None)
    }

    /// Stats for one index.
    pub fn get_index_stats(&self, kg: &str, name: &str) -> Result<Vec<IndexStats>, String> {
        index_commands::stats(&self.storage.read(), kg, Some(name))
    }

    /// Rebuild an index from base data, dropping tombstones.
    pub fn rebuild_index(&self, kg: &str, name: &str) -> Result<String, ProgramError> {
        index_commands::rebuild(&self.storage.read(), kg, name)
    }

    /// Execute an IQL program and return results.
    pub async fn query_program(
        &self,
        knowledge_graph: Option<String>,
        program: String,
    ) -> Result<QueryResult, String> {
        let control = self.request_control(None);
        self.run_program(knowledge_graph, program, None, &control)
            .await
            .map_err(|e| e.message)
    }

    /// `query_program` with `statements` already parsed by `parse_program`,
    /// under the request's deadline and cancellation.
    async fn run_program(
        &self,
        knowledge_graph: Option<String>,
        program: String,
        statements: Option<Vec<statement::Statement>>,
        control: &Arc<RequestControl>,
    ) -> Result<QueryResult, ProgramError> {
        self.run_job(
            self.make_query_job(),
            knowledge_graph,
            program,
            statements,
            control,
            &self.query_semaphore,
        )
        .await
    }

    /// Run `job` for `program` on the blocking pool under one of `permits`
    /// and the request's deadline and cancellation.
    async fn run_job(
        &self,
        job: QueryJob,
        knowledge_graph: Option<String>,
        program: String,
        statements: Option<Vec<statement::Statement>>,
        control: &Arc<RequestControl>,
        permits: &Arc<tokio::sync::Semaphore>,
    ) -> Result<QueryResult, ProgramError> {
        let program_len = program.len();
        let query_start = Instant::now();

        // Input size validation (WI-07)
        let perf = &self.config.storage.performance;
        if perf.max_query_size_bytes > 0 && program.len() > perf.max_query_size_bytes {
            return Err(format!(
                "Query too large: {} bytes (max {})",
                program.len(),
                perf.max_query_size_bytes
            )
            .into());
        }

        // A compute permit bounds concurrent computations at ncpu (spawn_blocking
        // alone would allow unlimited parallelism and CPU thrash). Waiting for
        // it, queueing on the blocking pool and computing all count against the
        // request's one deadline; see `supervise`.
        #[cfg(test)]
        let hook = test_hook::take();
        let run = move || {
            #[cfg(test)]
            let _hook = test_hook::install(hook);
            job.execute(knowledge_graph, program, statements)
                .map_err(ProgramError::from)
        };
        let result = supervise::run_blocking(permits, control, run).await;
        let compute_ms = query_start.elapsed().as_millis() as u64;
        info!(
            program_len,
            compute_ms,
            ok = result.is_ok(),
            "query_complete"
        );
        let slow_ms = self.config.storage.performance.slow_query_log_ms;
        if slow_ms > 0 && compute_ms >= slow_ms {
            warn!(
                program_len,
                compute_ms,
                threshold_ms = slow_ms,
                "slow_query"
            );
        }
        result
    }
}

impl QueryJob {
    /// Execute an IQL program synchronously on the current thread.
    /// Called from `Handler::query_program` via `tokio::task::spawn_blocking`
    /// so that Tokio worker threads are never blocked by DD computation.
    /// `statements`, when given, must be `parse_program(&program)`'s output.
    fn execute(
        self,
        knowledge_graph: Option<String>,
        program: String,
        statements: Option<Vec<statement::Statement>>,
    ) -> Result<QueryResult, String> {
        self.inc_query_count();
        let start = Instant::now();
        let program_len = program.len();
        info!(program_len, "query_job_start");

        // Use READ lock - all operations use _on()/_into() variants with explicit KG name.
        // This allows concurrent queries to execute without blocking each other.
        // KgDrop and RuleClear use interior mutability, so no write lock is needed.
        // `mut` is needed because MetaCommand::Compact releases and re-acquires the lock.
        let mut storage = self.storage.read();

        if knowledge_graph.as_deref() == Some(crate::auth::INTERNAL_KG) {
            return Err(internal_kg_denied());
        }

        // Determine target knowledge graph name
        let mut kg_name = if let Some(ref kg) = knowledge_graph {
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

        // Strip comment lines, then join indented continuation lines so that
        // multi-line rules (e.g., rule body on indented next line) become single
        // logical lines for the statement-per-line parser.
        let program_text = join_continuation_lines(&strip_comments(&program));

        // Phase 1: Parse-all-first validation.
        // If ANY statement fails to parse, reject the ENTIRE program with
        // structured error info, so nothing executes partially.
        let parse_start = Instant::now();
        let statements = match statements.map_or_else(|| parse_program(&program), Ok) {
            Ok(statements) => statements,
            Err(parse_errors) => {
                let errors_json = serde_json::to_string(&parse_errors).unwrap_or_default();
                return Err(format!("{VALIDATION_ERROR_PREFIX}{errors_json}"));
            }
        };
        info!(
            program_len,
            statements = statements.len(),
            parse_ms = parse_start.elapsed().as_millis() as u64,
            "query_parse_complete"
        );
        if kg_name == crate::auth::INTERNAL_KG || statements.iter().any(targets_internal_kg) {
            return Err(internal_kg_denied());
        }
        // A program that writes commits all its writes as one transaction;
        // a statement that cannot join it fails the program before anything runs.
        let transactional = program_boundary::is_transactional(&statements);
        // A proof that ends a program that writes runs after the commit, on
        // the snapshot the writes committed against.
        let pins_proof = program_boundary::has_pinned_proof(&statements);
        if let Err(violation) = program_boundary::check(&statements) {
            let text = program_text
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .nth(violation.index)
                .unwrap_or_default();
            let message = violation.message(text);
            return Ok(QueryResult {
                errors: vec![StatementError {
                    index: violation.index,
                    code: ErrorCode::Unsupported,
                    message: message.clone(),
                }],
                execution_time_ms: start.elapsed().as_millis() as u64,
                ..Handler::messages_result(vec![message])
            });
        }
        let mut statements = statements.into_iter().enumerate();

        // Phase 2: Execute statements (all guaranteed to parse successfully)
        let mut messages = Vec::new();
        // The query to run after the statements, with its statement index.
        let mut query_to_execute: Option<(usize, String)> = None;
        let mut current_stmt = String::new();
        // Track KG switch for WS session binding update
        let mut switched_kg_result: Option<String> = None;
        // Collect session facts (non-persisted) to temporarily insert before query
        // Format: (relation_name, tuple_values)
        let mut session_fact_tuples: Vec<(String, Tuple)> = Vec::new();
        // Collect session rules to prepend to queries
        let mut session_rules: Vec<String> = Vec::new();
        // Parsed session rules for validation (arity/aggregation compatibility)
        let mut session_rules_parsed: Vec<crate::ast::Rule> = Vec::new();
        let mut errors: Vec<StatementError> = Vec::new();
        let mut stmt_index: usize;
        // Records a failure of the current statement and reports it as a
        // message row too.
        macro_rules! fail {
            ($code:expr, $message:expr) => {{
                let message: String = $message;
                errors.push(StatementError {
                    index: stmt_index,
                    code: $code,
                    message: message.clone(),
                });
                messages.push(message);
            }};
        }

        // A proof reads only the snapshot captured under `storage`, so the
        // guard is released before proof search; a failed proof re-acquires
        // it for the statements that follow. A pinned proof waits for the
        // commit.
        let timing_mode = self.config.storage.performance.timing_mode;
        let mut pinned_proof: Option<(usize, Proof)> = None;
        macro_rules! run_proof {
            ($kg:expr, $proof:expr) => {{
                let proof: Proof = $proof;
                if pins_proof {
                    pinned_proof = Some((stmt_index, proof));
                } else {
                    let label = proof.label();
                    match ProofSnapshot::capture(&storage, $kg) {
                        Ok(snapshot) => {
                            drop(storage);
                            #[cfg(test)]
                            test_hook::run(test_hook::Point::ProofSearch);
                            match snapshot.explain(proof, timing_mode) {
                                Ok(qr) => return Ok(QueryResult { errors, ..qr }),
                                Err(e) => {
                                    storage = self.storage.read();
                                    fail!(
                                        supervise::computation_failure_code(ErrorCode::Validation),
                                        format!("{label}: {e}")
                                    );
                                }
                            }
                        }
                        Err(e) => fail!(ErrorCode::Validation, format!("{label}: {e}")),
                    }
                }
            }};
        }

        // Writes queue here and commit as one transaction after the last
        // statement.
        let mut write_run = WriteRun::default();
        macro_rules! queue {
            ($statement:expr) => {
                write_run.queue(stmt_index, $statement, &mut messages)
            };
        }

        let stmt_exec_start = Instant::now();
        for line in program_text.lines() {
            // A failed statement fails a transactional program: stop there.
            if transactional && !errors.is_empty() {
                break;
            }
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            current_stmt.push_str(line);
            current_stmt.push(' ');

            {
                let stmt_text = current_stmt.trim();
                if !stmt_text.is_empty() {
                    if let Some((index, stmt)) = statements.next() {
                        stmt_index = index;
                        if let Err(stop) = supervise::statement_gate(&stmt) {
                            return Err(stop.message().to_string());
                        }
                        match stmt {
                            statement::Statement::SchemaDecl(decl) => {
                                queue!(WriteStatement::Catalog(CatalogStatement::Schema(decl)));
                            }
                            statement::Statement::Insert(op) => {
                                queue!(WriteStatement::Facts(FactStatement::Insert(op)));
                            }
                            statement::Statement::Fact(rule) => {
                                // Session facts are NOT persisted - they are only available for
                                // queries during this request. Use +relation(args). to persist.
                                let tuple = match session_fact_tuple(&rule) {
                                    Ok(tuple) => tuple,
                                    Err(err) => {
                                        fail!(ErrorCode::Validation, err);
                                        current_stmt.clear();
                                        continue;
                                    }
                                };
                                session_fact_tuples.push((rule.head.relation.clone(), tuple));
                                messages.push(format!(
                                    "Session fact added for '{}'. (Use +{}(...) to persist)",
                                    rule.head.relation, rule.head.relation
                                ));
                            }
                            statement::Statement::Delete(op) => {
                                queue!(WriteStatement::Facts(FactStatement::Delete(op)));
                            }
                            statement::Statement::PersistentRule(rule) => {
                                queue!(WriteStatement::Catalog(CatalogStatement::Rule(rule)));
                            }
                            statement::Statement::SessionRule(rule) => {
                                if let Err(err) = check_session_rule(
                                    &storage,
                                    Some(&kg_name),
                                    &session_rules_parsed,
                                    &rule,
                                ) {
                                    fail!(ErrorCode::Validation, err);
                                    current_stmt.clear();
                                    continue;
                                }

                                let rule_text = format_rule_text(&rule);
                                session_rules.push(rule_text.clone());
                                session_rules_parsed.push(rule.clone());
                                messages.push(format!(
                                    "Session rule added for '{}'.",
                                    rule.head.relation
                                ));
                            }
                            statement::Statement::Query(_) => {
                                query_to_execute = Some((stmt_index, stmt_text.to_string()));
                            }
                            statement::Statement::DeleteRelationOrRule(name) => {
                                queue!(WriteStatement::Catalog(CatalogStatement::DropRule(name)));
                            }
                            statement::Statement::Update(op) => {
                                queue!(WriteStatement::Facts(FactStatement::Update(op)));
                            }
                            statement::Statement::TypeDecl(decl) => {
                                messages.push(format!("Type '{}' declared.", decl.name));
                            }
                            statement::Statement::Meta(meta) => {
                                let kg = kg_name.as_str();
                                #[cfg(test)]
                                test_hook::run(test_hook::Point::MetaDispatch);
                                match meta {
                                    // === Knowledge Graph commands ===
                                    MetaCommand::KgShow => {
                                        messages.push(format!("Current knowledge graph: {kg}"));
                                    }
                                    MetaCommand::KgList => {
                                        let kgs: Vec<_> = storage
                                            .list_knowledge_graphs()
                                            .into_iter()
                                            .filter(|name| name != crate::auth::INTERNAL_KG)
                                            .collect();
                                        if kgs.is_empty() {
                                            messages.push("No knowledge graphs found.".to_string());
                                        } else {
                                            messages.push("Knowledge Graphs:".to_string());
                                            for name in &kgs {
                                                let marker = if name == kg { " *" } else { "" };
                                                messages.push(format!("  {name}{marker}"));
                                            }
                                        }
                                    }
                                    MetaCommand::KgCreate(name) => {
                                        info!(kg = %name, "meta_kg_create_start");
                                        match storage.create_knowledge_graph(&name) {
                                            Ok(()) => {
                                                info!(kg = %name, "meta_kg_create_ok");
                                                self.notify_kg_change(&name, "created");
                                                messages.push(format!(
                                                    "Knowledge graph '{name}' created."
                                                ));
                                                messages.push(format!(
                                                    "Switched to knowledge graph: {name}"
                                                ));
                                                kg_name.clone_from(&name);
                                                switched_kg_result = Some(name);
                                            }
                                            Err(e) => {
                                                info!(kg = %name, error = %e, "meta_kg_create_err");
                                                fail!(
                                                    storage_error_code(&e, ErrorCode::Internal),
                                                    format!("Create failed: {e}")
                                                );
                                            }
                                        }
                                    }
                                    MetaCommand::KgUse(name) => {
                                        info!(kg = %name, "meta_kg_use_start");
                                        match storage.ensure_knowledge_graph(&name) {
                                            Ok(()) => {
                                                info!(kg = %name, "meta_kg_use_ok");
                                                messages.push(format!(
                                                    "Switched to knowledge graph: {name}"
                                                ));
                                                kg_name.clone_from(&name);
                                                switched_kg_result = Some(name);
                                            }
                                            Err(e) => {
                                                info!(kg = %name, error = %e, "meta_kg_use_err");
                                                fail!(
                                                    storage_error_code(&e, ErrorCode::Internal),
                                                    format!(
                                                        "Knowledge graph '{name}' not found: {e}"
                                                    )
                                                );
                                            }
                                        }
                                    }
                                    MetaCommand::KgDrop(name) => {
                                        if name == kg {
                                            fail!(
                                                ErrorCode::Conflict,
                                                "Cannot drop current knowledge graph. Switch to another first.".to_string()
                                            );
                                        } else {
                                            info!(kg = %name, "meta_kg_drop_start");
                                            // Phase 1: Fast in-memory removal (~microseconds).
                                            // Uses interior mutability (DashMap + RwLock), so no
                                            // storage-level write lock is needed. This avoids
                                            // the lock convoy that previously froze the server.
                                            match storage.prepare_drop_knowledge_graph(&name) {
                                                Ok(cleanup) => {
                                                    info!(kg = %name, "meta_kg_drop_prepare_ok");
                                                    messages.push(format!(
                                                        "Knowledge graph '{name}' dropped."
                                                    ));
                                                    // Phase 2: Slow file cleanup
                                                    storage.finish_drop_knowledge_graph(cleanup);
                                                    info!(kg = %name, "meta_kg_drop_finish_ok");
                                                    self.notify_kg_change(&name, "dropped");
                                                }
                                                Err(e) => {
                                                    info!(kg = %name, error = %e, "meta_kg_drop_err");
                                                    fail!(
                                                        storage_error_code(&e, ErrorCode::Internal),
                                                        format!("Drop failed: {e}")
                                                    );
                                                }
                                            }
                                        }
                                    }

                                    // === Relation commands ===
                                    MetaCommand::RelList => {
                                        match storage.list_relations_with_typed_metadata_in(kg) {
                                            Ok(relations) => {
                                                if relations.is_empty() {
                                                    messages.push(
                                                        "No relations in current knowledge graph."
                                                            .to_string(),
                                                    );
                                                } else {
                                                    let mut sorted = relations;
                                                    sorted.sort_by(|a, b| a.0.cmp(&b.0));
                                                    messages.push("Relations:".to_string());
                                                    for (name, typed_cols, count) in &sorted {
                                                        let cols = typed_cols
                                                            .iter()
                                                            .map(|(n, t)| format!("{n}: {t}"))
                                                            .collect::<Vec<_>>()
                                                            .join(", ");
                                                        messages.push(format!(
                                                            "  {name} (arity: {}, columns: [{cols}], tuples: {count})",
                                                            typed_cols.len()
                                                        ));
                                                    }
                                                }
                                            }
                                            Err(e) => fail!(
                                                storage_error_code(&e, ErrorCode::Internal),
                                                format!("Error: {e}")
                                            ),
                                        }
                                    }
                                    MetaCommand::RelDescribe(name) => {
                                        // Get metadata to determine arity
                                        match storage.get_relation_metadata_in(kg, &name) {
                                            Ok(Some((schema, total_count))) => {
                                                if schema.is_empty() {
                                                    messages.push(format!(
                                                        "Relation '{name}' is empty."
                                                    ));
                                                } else {
                                                    let arity = schema.len();
                                                    let vars: Vec<String> = (0..arity)
                                                        .map(|i| {
                                                            let letter =
                                                                (b'A' + (i % 26) as u8) as char;
                                                            let suffix = i / 26;
                                                            if suffix == 0 {
                                                                letter.to_string()
                                                            } else {
                                                                format!("{letter}{suffix}")
                                                            }
                                                        })
                                                        .collect();
                                                    // Execute query to get data (limit 10)
                                                    let query_text =
                                                        format!("?{name}({})", vars.join(", "));
                                                    query_to_execute =
                                                        Some((stmt_index, query_text));
                                                    messages.push(format!("Relation '{name}': {arity} columns, {total_count} total tuples"));
                                                }
                                            }
                                            Ok(None) => fail!(
                                                ErrorCode::NotFound,
                                                format!("Relation '{name}' not found.")
                                            ),
                                            Err(e) => fail!(
                                                storage_error_code(&e, ErrorCode::Internal),
                                                format!("Error: {e}")
                                            ),
                                        }
                                    }

                                    MetaCommand::RelDrop(name) => {
                                        match storage.drop_relation_in(kg, &name) {
                                            Ok(()) => {
                                                self.notify_schema_change(kg, &name, "dropped");
                                                messages
                                                    .push(format!("Relation '{name}' dropped."));
                                            }
                                            Err(e) => fail!(
                                                storage_error_code(&e, ErrorCode::NotFound),
                                                format!("Error: {e}")
                                            ),
                                        }
                                    }

                                    // === Rule commands ===
                                    MetaCommand::RuleList => match storage.list_rules_in(kg) {
                                        Ok(rules) => {
                                            if rules.is_empty() {
                                                messages.push("No rules defined.".to_string());
                                            } else {
                                                messages.push("Rules:".to_string());
                                                for name in &rules {
                                                    let clause_count = storage
                                                        .rule_count_in(kg, name)
                                                        .ok()
                                                        .flatten()
                                                        .unwrap_or(0);
                                                    messages.push(format!(
                                                        "  {name} ({clause_count} clause(s))"
                                                    ));
                                                }
                                            }
                                        }
                                        Err(e) => fail!(
                                            storage_error_code(&e, ErrorCode::Internal),
                                            format!("Error: {e}")
                                        ),
                                    },
                                    MetaCommand::RuleDrop(name) => {
                                        queue!(WriteStatement::Catalog(
                                            CatalogStatement::RuleDrop(name)
                                        ));
                                    }
                                    MetaCommand::RuleDropPrefix(prefix) => {
                                        queue!(WriteStatement::Catalog(
                                            CatalogStatement::RuleDropPrefix(prefix)
                                        ));
                                    }
                                    MetaCommand::RuleQuery(name) => {
                                        // Execute as a query - delegate to query path
                                        let query_text = format!("?{name}(X, Y)");
                                        query_to_execute = Some((stmt_index, query_text));
                                    }
                                    MetaCommand::RuleShowDef(name) => {
                                        match storage.describe_rule_in(kg, &name) {
                                            Ok(Some(desc)) => messages.push(desc),
                                            Ok(None) => {
                                                fail!(
                                                    ErrorCode::NotFound,
                                                    format!("Rule '{name}' not found.")
                                                );
                                            }
                                            Err(e) => fail!(
                                                storage_error_code(&e, ErrorCode::Internal),
                                                format!("Error: {e}")
                                            ),
                                        }
                                    }
                                    MetaCommand::RuleRemove { name, index } => {
                                        queue!(WriteStatement::Catalog(
                                            CatalogStatement::RuleRemove { name, index }
                                        ));
                                    }
                                    MetaCommand::RuleClear(name) => {
                                        queue!(WriteStatement::Catalog(
                                            CatalogStatement::RuleClear(name)
                                        ));
                                    }
                                    MetaCommand::RuleEdit { .. } => {
                                        fail!(
                                            ErrorCode::Unsupported,
                                            "Rule editing is not supported in server mode."
                                                .to_string()
                                        );
                                    }

                                    // === Clear commands ===
                                    MetaCommand::ClearPrefix(prefix) => {
                                        match storage.clear_relations_by_prefix_in(kg, &prefix) {
                                            Ok(cleared) => {
                                                if cleared.is_empty() {
                                                    messages.push(format!(
                                                        "No relations matching prefix '{prefix}'."
                                                    ));
                                                } else {
                                                    for (name, count) in &cleared {
                                                        self.notify_persistent_update(
                                                            kg, name, "delete", *count,
                                                        );
                                                    }
                                                    let total: usize =
                                                        cleared.iter().map(|(_, c)| c).sum();
                                                    let detail: Vec<String> = cleared
                                                        .iter()
                                                        .map(|(name, count)| {
                                                            format!("{name} ({count})")
                                                        })
                                                        .collect();
                                                    messages.push(format!(
                                                        "Cleared {} fact(s) from {} relation(s) with prefix '{prefix}': {}",
                                                        total, cleared.len(), detail.join(", ")
                                                    ));
                                                }
                                            }
                                            Err(e) => fail!(
                                                storage_error_code(&e, ErrorCode::Internal),
                                                format!("Error: {e}")
                                            ),
                                        }
                                    }

                                    // === System commands ===
                                    MetaCommand::Status => {
                                        let kgs = storage.list_knowledge_graphs();
                                        let uptime = self.uptime_seconds();
                                        let queries = self.total_queries();
                                        messages.push("Server Status".to_string());
                                        messages.push("  Health: healthy".to_string());
                                        messages.push(format!(
                                            "  Version: {}",
                                            env!("CARGO_PKG_VERSION")
                                        ));
                                        messages.push(format!("  Uptime: {uptime} seconds"));
                                        messages.push(format!("  Total queries: {queries}"));
                                        messages.push(format!("  Knowledge graphs: {}", kgs.len()));
                                    }
                                    MetaCommand::Compact => {
                                        // Release storage read lock BEFORE heavy I/O.
                                        // compact_all() performs disk I/O (rewrite batch files,
                                        // sync, save metadata). Holding the read lock during this
                                        // blocks all writes due to parking_lot's write-preferring
                                        // policy, causing a server freeze.
                                        drop(storage);
                                        let compact_result = {
                                            let s = self.storage.read();
                                            s.compact_all()
                                        }; // read lock released here, before push
                                           // Re-acquire for any subsequent statements
                                        storage = self.storage.read();
                                        match compact_result {
                                            Ok(()) => {
                                                messages.push("Compaction complete.".to_string());
                                            }
                                            Err(e) => {
                                                fail!(
                                                    storage_error_code(&e, ErrorCode::Internal),
                                                    format!("Compaction error: {e}")
                                                );
                                            }
                                        }
                                    }
                                    MetaCommand::Backup(name) => {
                                        match backup_command::start(&storage, name.as_deref()) {
                                            Ok(rows) => messages.extend(rows),
                                            Err((code, message)) => fail!(code, message),
                                        }
                                    }
                                    MetaCommand::BackupStatus => {
                                        messages.extend(backup_command::status(&storage));
                                    }

                                    // === Debug command ===
                                    MetaCommand::Debug(query) => {
                                        // Transform ?shorthand before debug
                                        let debug_src = match transform_query_shorthand(&query) {
                                            Ok(t) => t.query,
                                            Err(_) => query,
                                        };
                                        let debug_result =
                                            debug_query(&storage, Some(kg), &debug_src);
                                        match debug_result {
                                            Ok((plan, optimizations)) => {
                                                messages.push("Query Plan:".to_string());
                                                messages.push(plan);
                                                messages.push(String::new());
                                                messages.push("Optimization passes:".to_string());
                                                for opt in &optimizations {
                                                    messages.push(format!("  - {opt}"));
                                                }
                                            }
                                            Err(e) => {
                                                fail!(
                                                    ErrorCode::Validation,
                                                    format!("Debug error: {e}")
                                                );
                                            }
                                        }
                                    }

                                    // === Why (proof tree) command ===
                                    MetaCommand::Why(query) => {
                                        let why_q = match transform_query_shorthand(&query) {
                                            Ok(t) => t.query,
                                            Err(_) => query,
                                        };
                                        run_proof!(
                                            kg,
                                            Proof::Why {
                                                query: why_q,
                                                full: false
                                            }
                                        );
                                    }
                                    MetaCommand::WhyFull(query) => {
                                        let why_q = match transform_query_shorthand(&query) {
                                            Ok(t) => t.query,
                                            Err(_) => query,
                                        };
                                        run_proof!(
                                            kg,
                                            Proof::Why {
                                                query: why_q,
                                                full: true
                                            }
                                        );
                                    }

                                    // === Why Not (negative explanation) command ===
                                    MetaCommand::WhyNot(input) => {
                                        run_proof!(kg, Proof::WhyNot(input));
                                    }

                                    // === Index commands ===
                                    MetaCommand::IndexCreate(opts) => {
                                        info!(index = %opts.name, "meta_index_create_start");
                                        match index_commands::create(&storage, kg, &opts) {
                                            Ok(msg) => {
                                                info!(index = %opts.name, "meta_index_create_ok");
                                                messages.push(msg);
                                            }
                                            Err(e) => {
                                                info!(index = %opts.name, error = %e, "meta_index_create_err");
                                                fail!(
                                                    e.code
                                                        .filter(|code| matches!(
                                                            code,
                                                            ErrorCode::OutcomeUnknown
                                                                | ErrorCode::StoreReadOnly
                                                        ))
                                                        .unwrap_or_else(|| index_error_code(
                                                            &storage,
                                                            kg,
                                                            &opts.name,
                                                            ErrorCode::Validation,
                                                            ErrorCode::Conflict
                                                        )),
                                                    format!("Index error: {e}")
                                                );
                                            }
                                        }
                                    }
                                    MetaCommand::IndexDrop(name) => {
                                        info!(index = %name, "meta_index_drop_start");
                                        match index_commands::drop(&storage, kg, &name) {
                                            Ok(msg) => {
                                                info!(index = %name, "meta_index_drop_ok");
                                                messages.push(msg);
                                            }
                                            Err(e) => {
                                                info!(index = %name, error = %e, "meta_index_drop_err");
                                                fail!(
                                                    e.code
                                                        .filter(|code| matches!(
                                                            code,
                                                            ErrorCode::OutcomeUnknown
                                                                | ErrorCode::StoreReadOnly
                                                        ))
                                                        .unwrap_or_else(|| index_error_code(
                                                            &storage,
                                                            kg,
                                                            &name,
                                                            ErrorCode::NotFound,
                                                            ErrorCode::Internal
                                                        )),
                                                    format!("Index error: {e}")
                                                );
                                            }
                                        }
                                    }
                                    MetaCommand::IndexList => {
                                        match index_commands::stats(&storage, kg, None) {
                                            Ok(stats) => {
                                                info!(count = stats.len(), "meta_index_list_ok");
                                                if stats.is_empty() {
                                                    messages.push("No indexes.".to_string());
                                                } else {
                                                    for s in &stats {
                                                        messages.push(format!(
                                                            "Index '{}' on {}.{} (type: {}, metric: {}, vectors: {})",
                                                            s.name, s.relation, s.column,
                                                            s.index_type, s.metric,
                                                            s.tuple_count
                                                        ));
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                info!(error = %e, "meta_index_list_err");
                                                fail!(
                                                    ErrorCode::Internal,
                                                    format!("Index error: {e}")
                                                );
                                            }
                                        }
                                    }
                                    MetaCommand::IndexStats(name) => {
                                        info!(index = %name, "meta_index_stats_start");
                                        match index_commands::stats(&storage, kg, Some(&name)) {
                                            Ok(stats) => {
                                                info!(index = %name, count = stats.len(), "meta_index_stats_ok");
                                                for s in &stats {
                                                    messages.push(format!(
                                                        "Index '{}': relation={}, column={}, type={}, metric={}, vectors={}, tombstones={}, dimension={}",
                                                        s.name, s.relation, s.column,
                                                        s.index_type, s.metric,
                                                        s.tuple_count, s.tombstone_count,
                                                        s.dimension
                                                    ));
                                                }
                                            }
                                            Err(e) => fail!(
                                                index_error_code(
                                                    &storage,
                                                    kg,
                                                    &name,
                                                    ErrorCode::NotFound,
                                                    ErrorCode::Internal
                                                ),
                                                format!("Index error: {e}")
                                            ),
                                        }
                                    }
                                    MetaCommand::IndexRebuild(name) => {
                                        match index_commands::rebuild(&storage, kg, &name) {
                                            Ok(msg) => messages.push(msg),
                                            Err(e) => fail!(
                                                e.code
                                                    .filter(|code| matches!(
                                                        code,
                                                        ErrorCode::OutcomeUnknown
                                                            | ErrorCode::StoreReadOnly
                                                    ))
                                                    .unwrap_or_else(|| index_error_code(
                                                        &storage,
                                                        kg,
                                                        &name,
                                                        ErrorCode::NotFound,
                                                        ErrorCode::Internal
                                                    )),
                                                format!("Index error: {e}")
                                            ),
                                        }
                                    }

                                    // === Session commands (handled by execute_program) ===
                                    MetaCommand::SessionList
                                    | MetaCommand::SessionClear
                                    | MetaCommand::SessionDrop(_)
                                    | MetaCommand::SessionDropName(_) => {
                                        fail!(
                                            ErrorCode::Unsupported,
                                            "Session commands require a WebSocket connection."
                                                .to_string()
                                        );
                                    }

                                    // === User & API key management (handled by execute_program) ===
                                    MetaCommand::UserList
                                    | MetaCommand::UserCreate { .. }
                                    | MetaCommand::UserDrop(_)
                                    | MetaCommand::UserPassword { .. }
                                    | MetaCommand::UserRole { .. }
                                    | MetaCommand::ApiKeyCreate { .. }
                                    | MetaCommand::ApiKeyList
                                    | MetaCommand::ApiKeyRevoke(_)
                                    | MetaCommand::ApiKeyExpire { .. } => {
                                        fail!(
                                            ErrorCode::Unsupported,
                                            "User/API key commands require a WebSocket connection with admin privileges."
                                                .to_string()
                                        );
                                    }

                                    // === KG ACL commands ===
                                    MetaCommand::KgAclList(_)
                                    | MetaCommand::KgAclGrant { .. }
                                    | MetaCommand::KgAclRevoke { .. } => {
                                        fail!(
                                            ErrorCode::Unsupported,
                                            "KG ACL commands require a WebSocket connection with admin or owner privileges."
                                                .to_string()
                                        );
                                    }

                                    // Handled asynchronously at the
                                    // execute_program level (registry
                                    // fetch); reaching this arm means the
                                    // command was embedded in a
                                    // multi-statement program.
                                    MetaCommand::OntologyInstall(_)
                                    | MetaCommand::OntologyRemove(_)
                                    | MetaCommand::OntologyUpgrade(_) => {
                                        fail!(
                                            ErrorCode::Unsupported,
                                            ".ontology commands must be run as a standalone statement."
                                                .to_string()
                                        );
                                    }

                                    // Owned by the /ws connection, which
                                    // intercepts them before execution.
                                    MetaCommand::Subscribe { .. } | MetaCommand::Unsubscribe(_) => {
                                        fail!(
                                            ErrorCode::Unsupported,
                                            SUBSCRIPTION_WS_ONLY.to_string()
                                        );
                                    }

                                    // === Client-only commands ===
                                    MetaCommand::Help
                                    | MetaCommand::Quit
                                    | MetaCommand::Load { .. } => {
                                        fail!(
                                            ErrorCode::Unsupported,
                                            "This command is client-only and not available via server API.".to_string()
                                        );
                                    }
                                }
                            }
                        }
                    } else {
                        return Err("Internal error: statement count mismatch".to_string());
                    }
                }
                current_stmt.clear();
            }
        }
        // Counts of the fact statements committed below.
        let mut statement_counts = Vec::new();
        let mut committed = None;
        // The revision the program's writes committed at, if they did.
        let mut revision = None;
        if !write_run.is_empty() {
            if errors.is_empty() {
                match self.commit_write_run(&storage, &kg_name, &mut write_run, &mut messages) {
                    Ok((base, counts, committed_at)) => {
                        committed = Some(base);
                        statement_counts = counts;
                        revision = Some(committed_at);
                    }
                    Err(failure) => {
                        stmt_index = failure.index;
                        fail!(failure.code, failure.message);
                    }
                }
            } else if let Some(note) = write_run.abandon(errors[0].index) {
                // The loop stopped at the first failure, so it is the last row.
                errors[0].message.push_str(&note);
                if let Some(row) = messages.last_mut() {
                    row.push_str(&note);
                }
            }
        }
        write_run.remove_vacant(&mut messages);
        let stmt_exec_ms = stmt_exec_start.elapsed().as_millis() as u64;
        if stmt_exec_ms > 0 {
            info!(
                program_len,
                stmt_exec_ms,
                session_facts = session_fact_tuples.len(),
                session_rules = session_rules.len(),
                "query_statement_exec_complete"
            );
        }

        // A pinned proof explains the snapshot the program's writes committed
        // against: the state its guard passed on. The reply keeps the
        // statements' messages and carries the proof's trees. A failed proof
        // fails its statement; the writes stay committed.
        if let (Some((index, proof)), Some(base), true) =
            (pinned_proof, committed, errors.is_empty())
        {
            let snapshot = ProofSnapshot::pinned(&storage, &kg_name, base);
            drop(storage);
            #[cfg(test)]
            test_hook::run(test_hook::Point::ProofSearch);
            let label = proof.label();
            let (mut proof_trees, mut timing_breakdown) = (None, None);
            match snapshot.and_then(|snapshot| snapshot.explain(proof, timing_mode)) {
                Ok(explained) => {
                    proof_trees = Some(explained.proof_trees.unwrap_or_default());
                    timing_breakdown = explained.timing_breakdown;
                }
                Err(e) => {
                    stmt_index = index;
                    fail!(
                        supervise::computation_failure_code(ErrorCode::Validation),
                        format!("{label}: {e}")
                    );
                }
            }
            return Ok(QueryResult {
                proof_trees,
                timing_breakdown,
                switched_kg: switched_kg_result,
                errors,
                statements: statement_counts,
                execution_time_ms: start.elapsed().as_millis() as u64,
                revision,
                ..Handler::messages_result(messages)
            });
        }

        // Return messages if no query, or if a transactional program failed:
        // its query does not run.
        if (!messages.is_empty() && query_to_execute.is_none())
            || (transactional && !errors.is_empty())
        {
            drop(storage); // Release storage lock - we no longer need it
            info!(
                program_len,
                total_ms = start.elapsed().as_millis() as u64,
                "query_job_complete_messages"
            );
            return Ok(QueryResult {
                switched_kg: switched_kg_result,
                errors,
                statements: statement_counts,
                execution_time_ms: start.elapsed().as_millis() as u64,
                revision,
                ..Handler::messages_result(messages)
            });
        }

        let (query_index, program_text) = match query_to_execute {
            Some((index, query)) => (Some(index), query),
            None => (None, program_text),
        };
        // A failed query is a failure of its statement; the statements before
        // it keep their results.
        macro_rules! fail_query {
            ($code:expr, $message:expr) => {{
                let message: String = $message;
                match query_index {
                    Some(index) => {
                        stmt_index = index;
                        fail!($code, message);
                        return Ok(QueryResult {
                            switched_kg: switched_kg_result,
                            errors,
                            statements: statement_counts,
                            execution_time_ms: start.elapsed().as_millis() as u64,
                            revision,
                            ..Handler::messages_result(messages)
                        });
                    }
                    None => return Err(message),
                }
            }};
        }

        // Transform ?shorthand query syntax into __query__(...) <- ... rule
        let transform = match transform_query_shorthand(&program_text) {
            Ok(transform) => transform,
            Err(e) => fail_query!(ErrorCode::Validation, e),
        };
        let query_program = transform.query;
        let order_by = transform.order_by;
        let query_limit = transform.limit;
        let query_offset = transform.offset;
        // Prepend session rules to the query program
        let query_program = if session_rules.is_empty() {
            query_program
        } else {
            let rules_text = session_rules.join("\n");
            format!("{rules_text}\n{query_program}")
        };

        // Get a snapshot of the KG data under the lock (O(1) Arc clone),
        // then RELEASE the storage lock before the heavy DD computation.
        // This prevents lock convoys: when another thread needs a write lock
        // (e.g. .kg drop), parking_lot's write-preferring policy would otherwise
        // block ALL new readers while waiting for long-running DD computations.
        // Look up registered schema column names before releasing the lock
        let schema_col_names: Option<Vec<String>> = find_query_source_relation(&query_program)
            .and_then(|rel| storage.get_schema_in(&kg_name, &rel).ok().flatten())
            .map(|s| s.columns.iter().map(|c| c.name.clone()).collect());

        let snapshot = match self
            .pinned
            .clone()
            .map_or_else(|| storage.get_snapshot_for(&kg_name), Ok)
        {
            Ok(snapshot) => snapshot,
            Err(e) => fail_query!(storage_error_code(&e, ErrorCode::Internal), e.to_string()),
        };
        drop(storage); // Release storage read lock BEFORE DD computation

        let debug_session = std::env::var("INPUTLAYER_DEBUG_SESSION").is_ok();
        if debug_session && !session_fact_tuples.is_empty() {
            debug!(
                count = session_fact_tuples.len(),
                "Executing with session facts (isolated)"
            );
            for (relation, tuple) in &session_fact_tuples {
                debug!(relation, tuple = ?tuple, "session_fact");
            }
        }

        // Execute DD computation on the snapshot - completely lock-free.
        // Session facts are added to an ISOLATED COPY, providing request-scoped isolation.
        let query_exec_start = Instant::now();
        let has_session_facts = !session_fact_tuples.is_empty();
        let timing_mode = self.config.storage.performance.timing_mode;
        let run = || {
            if has_session_facts {
                snapshot.execute_with_session_facts_profiled(
                    &query_program,
                    session_fact_tuples,
                    timing_mode,
                )
            } else if let Some(cache_plan) = &self.cache_plan {
                snapshot
                    .execute_with_rules_tuples_cached(&query_program, timing_mode)
                    .map(|(tuples, timing, run)| {
                        let _ = cache_plan.set(run);
                        (tuples, timing)
                    })
            } else {
                snapshot.execute_with_rules_tuples_profiled(&query_program, timing_mode)
            }
        };
        let needs_full = needs_full_result(&order_by, query_offset);
        let executed = if needs_full {
            crate::without_result_cap(run)
        } else {
            run()
        };
        let (results, timing_breakdown) = match executed {
            Ok(executed) => executed,
            Err(e) => fail_query!(
                supervise::computation_failure_code(ErrorCode::Validation),
                format!("Query execution failed: {e}")
            ),
        };
        let row_capped = crate::last_result_truncated();
        let query_exec_ms = query_exec_start.elapsed().as_millis() as u64;
        info!(
            program_len,
            kg = %kg_name,
            query_exec_ms,
            has_session_facts,
            "query_engine_exec_complete"
        );

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

        // Build schema from first result or default to 2 columns.
        // Prefer registered schema column names when the relation has a defined schema.
        // Fall back to query variable names only when no schema exists.
        let schema: Vec<ColumnDef> = if let Some(first) = results.first() {
            let arity = first.values().len();
            let col_names = if let Some(ref names) = schema_col_names {
                if names.len() == arity {
                    names.clone()
                } else {
                    extract_column_names_from_query(&query_program, arity)
                }
            } else {
                extract_column_names_from_query(&query_program, arity)
            };

            first
                .values()
                .iter()
                .enumerate()
                .map(|(i, v)| ColumnDef {
                    name: col_names[i].clone(),
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

        // Apply sorting if :asc/:desc annotations were present
        let rows = sort_rows(rows, &order_by);

        // Apply pagination (offset then limit)
        let total_count = rows.len();
        let rows = apply_pagination(rows, query_limit, query_offset);
        let (rows, cut) = cap_rows(rows, self.config.storage.performance.max_result_rows);
        let truncated = row_capped || cut || rows.len() < total_count;

        info!(
            program_len,
            total_ms = start.elapsed().as_millis() as u64,
            row_count = rows.len(),
            total_count,
            truncated,
            "query_job_complete"
        );

        // Record timing into Prometheus histograms.
        if let Some(ref tb) = timing_breakdown {
            self.timing_histograms.record(tb);
        }

        Ok(QueryResult {
            rows,
            schema,
            total_count,
            truncated,
            execution_time_ms: start.elapsed().as_millis() as u64,
            metadata: None,
            switched_kg: None,
            proof_trees: None,
            timing_breakdown,
            errors,
            statements: statement_counts,
            revision,
        })
    }
}

impl Handler {
    /// Debug a query plan without executing it.
    ///
    /// Runs the full compilation pipeline (parse → IR → optimize) and returns
    /// a human-readable representation of the query plan at each stage.
    pub fn debug_query(
        &self,
        knowledge_graph: Option<String>,
        query: String,
    ) -> Result<(String, Vec<String>), String> {
        debug_query(&self.storage.read(), knowledge_graph.as_deref(), &query)
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
        session_id: &SessionId,
        program: String,
    ) -> Result<QueryResult, String> {
        let control = self.request_control(None);
        self.run_program_with_session(session_id, None, program, None, &control)
            .await
            .map_err(|e| e.message)
    }

    /// `query_program_with_session` on `kg` instead of the session's binding,
    /// including when the session is gone, under the request's deadline and
    /// cancellation.
    async fn run_program_with_session(
        &self,
        session_id: &SessionId,
        kg: Option<String>,
        program: String,
        statements: Option<Vec<statement::Statement>>,
        control: &Arc<RequestControl>,
    ) -> Result<QueryResult, ProgramError> {
        // Input size validation (same as query_program)
        let perf = &self.config.storage.performance;
        if perf.max_query_size_bytes > 0 && program.len() > perf.max_query_size_bytes {
            return Err(format!(
                "Query too large: {} bytes (max {})",
                program.len(),
                perf.max_query_size_bytes
            )
            .into());
        }

        // Touch session to prevent idle reaping during query execution.
        // If session was reaped (e.g., WS reconnect), fall back to non-session query.
        if self.sessions.touch_session(session_id).is_err() {
            tracing::debug!(session_id = %session_id, "session_gone_fallback_to_query_program");
            return self.run_program(kg, program, statements, control).await;
        }

        // Check if session is clean → fast path
        let is_clean = self.sessions.is_session_clean(session_id)?;
        let kg = match kg {
            Some(kg) => kg,
            None => self.sessions.session_kg(session_id)?,
        };

        if is_clean {
            // Fast path: no ephemeral state, use global snapshot directly
            return self
                .run_program(Some(kg), program, statements, control)
                .await;
        }

        // Slow path: combine ephemeral + persistent data
        // Get ephemeral facts and rules from session
        let session_facts = self.sessions.get_session_facts(session_id)?;
        let rule_texts: Vec<String> = self
            .sessions
            .with_session(session_id, |session| session.rule_texts().to_vec())?;

        // Apply same preprocessing as the fast path: strip comments + transform ?shorthand
        let preprocessed = strip_comments(&program);
        let transform = transform_query_shorthand(&preprocessed)?;
        let preprocessed = transform.query;
        let order_by = transform.order_by;
        let query_limit = transform.limit;
        let query_offset = transform.offset;
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

        // Get snapshot under read lock, then RELEASE lock immediately.
        // This prevents lock convoys: holding the lock during DD computation
        // would block all mutations (the exact bug fixed in PR #12 for regular queries).
        let (snapshot, schema_col_names) = {
            let storage = self.storage.read();
            storage
                .ensure_knowledge_graph(&kg)
                .map_err(|e| format!("Knowledge graph not found: {e}"))?;
            let names: Option<Vec<String>> = find_query_source_relation(&preprocessed)
                .and_then(|rel| storage.get_schema_in(&kg, &rel).ok().flatten())
                .map(|s| s.columns.iter().map(|c| c.name.clone()).collect());
            let snap = storage.get_snapshot_for(&kg).map_err(|e| e.to_string())?;
            (snap, names)
        }; // storage read lock released here

        // Offload CPU-bound DD computation to the blocking thread pool, under
        // a compute permit and the request's deadline (same as query_program).
        let combined_program_clone = combined_program;
        let preprocessed_clone = preprocessed.clone();
        let timing_mode = self.config.storage.performance.timing_mode;
        let timing_histograms = Arc::clone(&self.timing_histograms);
        let needs_full = needs_full_result(&order_by, query_offset);
        let (results, row_capped, baseline, timing_breakdown) =
            supervise::run_blocking(&self.query_semaphore, control, move || {
                // Run session query on snapshot (lock-free) with profiling
                let run = || {
                    snapshot.execute_with_session_facts_profiled(
                        &combined_program_clone,
                        session_facts,
                        timing_mode,
                    )
                };
                let (results, timing_breakdown) = if needs_full {
                    crate::without_result_cap(run)
                } else {
                    run()
                }
                .map_err(|e| ProgramError::from(format!("Query execution failed: {e}")))?;
                let row_capped = crate::last_result_truncated();

                // Record timing in Prometheus histograms
                if let Some(ref tb) = timing_breakdown {
                    timing_histograms.record(tb);
                }

                // Per-tuple provenance: run the original query (without ephemeral rules)
                // against persistent-only data to identify ephemeral contributions.
                // Uncapped, so a capped result is never compared to a different subset.
                use std::collections::HashSet;
                let baseline: HashSet<Tuple> = if results.is_empty() {
                    HashSet::new()
                } else {
                    match crate::without_result_cap(|| {
                        snapshot.execute_with_rules_tuples(&preprocessed_clone)
                    }) {
                        Ok(tuples) => tuples.into_iter().collect(),
                        Err(e) => {
                            warn!(error = %e, "Provenance baseline query failed - all tuples tagged as ephemeral");
                            HashSet::new()
                        }
                    }
                };

                Ok((results, row_capped, baseline, timing_breakdown))
            })
            .await?;

        use crate::session::Provenance;

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
            let arity = first.values().len();
            let col_names = if let Some(ref names) = schema_col_names {
                if names.len() == arity {
                    names.clone()
                } else {
                    extract_column_names_from_query(&preprocessed, arity)
                }
            } else {
                extract_column_names_from_query(&preprocessed, arity)
            };

            first
                .values()
                .iter()
                .enumerate()
                .map(|(i, v)| ColumnDef {
                    name: col_names[i].clone(),
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

        // Slow query logging (same as query_program)
        let program_len = program.len();
        let slow_ms = self.config.storage.performance.slow_query_log_ms;
        if slow_ms > 0 && execution_time_ms >= slow_ms {
            warn!(
                program_len,
                compute_ms = execution_time_ms,
                threshold_ms = slow_ms,
                "slow_session_query"
            );
        }

        // Record audit event for query with ephemeral data
        if query_meta.has_ephemeral {
            self.sessions.record_query_with_ephemeral(
                session_id,
                query_meta.ephemeral_sources.clone(),
                rows.len(),
                execution_time_ms,
            );
        }

        // Apply sorting if :asc/:desc annotations were present
        let rows = sort_rows(rows, &order_by);

        // Apply pagination (offset then limit)
        let total_count = rows.len();
        let rows = apply_pagination(rows, query_limit, query_offset);
        let (rows, cut) = cap_rows(rows, self.config.storage.performance.max_result_rows);
        let truncated = row_capped || cut || rows.len() < total_count;

        Ok(QueryResult {
            rows,
            schema,
            total_count,
            truncated,
            execution_time_ms,
            metadata: result_metadata,
            switched_kg: None,
            proof_trees: None,
            timing_breakdown,
            errors: Vec::new(),
            statements: Vec::new(),
            revision: None,
        })
    }

    /// Validate a relation name for use in inserts/retracts.
    fn validate_relation_name(name: &str) -> Result<(), String> {
        crate::naming::validate_relation_name(name)
    }

    /// Insert ephemeral facts into a session.
    /// Returns the number of facts actually inserted (after dedup).
    pub fn session_insert_ephemeral(
        &self,
        session_id: &SessionId,
        relation: &str,
        tuples: Vec<Tuple>,
    ) -> Result<usize, String> {
        Self::validate_relation_name(relation)?;
        let max_tuples = self.config.storage.performance.max_insert_tuples;
        if max_tuples > 0 && tuples.len() > max_tuples {
            return Err(format!(
                "Too many tuples: {} (max {})",
                tuples.len(),
                max_tuples
            ));
        }
        // Validate arity against persistent schema (if one exists)
        if let Ok(kg) = self.sessions.session_kg(session_id) {
            self.validate_tuples_against_schema(&kg, relation, &tuples)?;
        }
        self.sessions.insert_ephemeral(session_id, relation, tuples)
    }

    /// Retract ephemeral facts from a session.
    pub fn session_retract_ephemeral(
        &self,
        session_id: &SessionId,
        relation: &str,
        tuples: Vec<Tuple>,
    ) -> Result<usize, String> {
        Self::validate_relation_name(relation)?;
        let max_tuples = self.config.storage.performance.max_insert_tuples;
        if max_tuples > 0 && tuples.len() > max_tuples {
            return Err(format!(
                "Too many tuples: {} (max {})",
                tuples.len(),
                max_tuples
            ));
        }
        self.sessions
            .retract_ephemeral(session_id, relation, tuples)
    }

    /// Add an ephemeral rule to a session.
    pub fn session_add_rule(
        &self,
        session_id: &SessionId,
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

    /// Execute a program with optional session context and auth identity.
    ///
    /// This is the unified entry point for the WebSocket protocol. It handles:
    /// - Authorization checks (when `auth` is `Some`)
    /// - Session commands (when `session_id` is `Some`)
    /// - KG switching with session binding updates
    /// - All other statements via `query_program()` or `query_program_with_session()`
    ///
    /// A one-statement program whose statement failed is an `Err`; a longer
    /// program reports failed statements in `QueryResult::errors`.
    ///
    /// `auth` is checked twice: at admission, where it yields the permission
    /// snapshot the whole program is authorized with, and at release, so a
    /// credential revoked while the program ran receives none of its output.
    /// Writes admitted before the revocation stay committed.
    pub async fn execute_program(
        &self,
        session_id: Option<&SessionId>,
        knowledge_graph: Option<String>,
        program: String,
        auth: Option<&crate::auth::Principal>,
    ) -> Result<QueryResult, ProgramError> {
        let control = self.request_control(None);
        self.execute_program_status(session_id, knowledge_graph, program, auth, &control)
            .await
    }

    /// `execute_program`, keeping the failed statement's `ErrorCode`, under
    /// `control`: the request's deadline (see [`Self::request_control`]) and
    /// cancellation. A request stopped before it began committing applied
    /// nothing and fails with `deadline_exceeded` or `cancelled`; once it began
    /// committing it runs to completion and returns what it committed.
    pub async fn execute_program_status(
        &self,
        session_id: Option<&SessionId>,
        knowledge_graph: Option<String>,
        program: String,
        auth: Option<&crate::auth::Principal>,
        control: &Arc<RequestControl>,
    ) -> Result<QueryResult, ProgramError> {
        let single_statement = program_statement_count(&program) == 1;
        let result = self
            .run_execute_program(session_id, knowledge_graph, program, auth, control)
            .await?;
        // A stop that won the race discards a result that changed nothing; a
        // later one is too late.
        control.finish().map_err(supervise::stop_error)?;
        settle_result(result, auth, single_statement)
    }

    /// The registered schema column names of the relation a query program
    /// (`__query__(...) <- ...`) reads first, if it has a schema: a result as
    /// wide as the schema takes its names.
    pub(crate) fn source_schema_columns(
        &self,
        knowledge_graph: &str,
        query_program: &str,
    ) -> Option<Vec<String>> {
        let relation = find_query_source_relation(query_program)?;
        let storage = self.storage.read();
        storage
            .get_schema_in(knowledge_graph, &relation)
            .ok()
            .flatten()
            .map(|schema| schema.columns.iter().map(|c| c.name.clone()).collect())
    }

    /// Check that `auth` may run the query `goal` on `knowledge_graph`: the
    /// check `execute_program` makes before running it.
    pub fn authorize_query(
        &self,
        auth: Option<&crate::auth::Principal>,
        knowledge_graph: &str,
        goal: &crate::statement::QueryGoal,
    ) -> Result<(), String> {
        let identity = auth
            .map(crate::auth::Principal::identity)
            .transpose()
            .map_err(String::from)?;
        self.authorize_program(
            identity.as_ref(),
            Some(knowledge_graph),
            &[statement::Statement::Query(goal.clone())],
        )
    }

    /// Run the query `query` (`?body`) on `snapshot`, a snapshot of
    /// `knowledge_graph`, as `auth` would run it with `execute_program`:
    /// admitted and authorized the same way, but reading `snapshot` instead of
    /// whatever is current when the query runs. The result is therefore the
    /// query's exact answer at `snapshot.revision`. The query's compiled plan
    /// is kept and reused on later snapshots until the rules change; the
    /// [`CachedRun`](crate::storage_engine::CachedRun) tells whether this run
    /// reused it and how long executing it took. A `probe` (of standing-query
    /// sharing) computes under a probe permit instead of a compute permit.
    pub async fn query_snapshot(
        &self,
        knowledge_graph: &str,
        snapshot: Arc<KnowledgeGraphSnapshot>,
        query: &str,
        auth: Option<&crate::auth::Principal>,
        probe: bool,
    ) -> Result<(QueryResult, crate::storage_engine::CachedRun), String> {
        let identity = auth
            .map(crate::auth::Principal::identity)
            .transpose()
            .map_err(String::from)?;
        let statements = parse_program(query).map_err(|errors| {
            let errors_json = serde_json::to_string(&errors).unwrap_or_default();
            format!("{VALIDATION_ERROR_PREFIX}{errors_json}")
        })?;
        if !matches!(statements.as_slice(), [statement::Statement::Query(_)]) {
            return Err("A snapshot query must be a single query".to_string());
        }
        self.authorize_program(identity.as_ref(), Some(knowledge_graph), &statements)?;
        let cache_plan = Arc::new(std::sync::OnceLock::new());
        let job = QueryJob {
            pinned: Some(snapshot),
            cache_plan: Some(Arc::clone(&cache_plan)),
            ..self.make_query_job()
        };
        let control = self.request_control(None);
        let result = self
            .run_job(
                job,
                Some(knowledge_graph.to_string()),
                query.to_string(),
                Some(statements),
                &control,
                if probe {
                    &self.probe_semaphore
                } else {
                    &self.query_semaphore
                },
            )
            .await?;
        let result = settle_result(result, auth, true).map_err(|e| e.message)?;
        Ok((result, cache_plan.get().copied().unwrap_or_default()))
    }

    async fn run_execute_program(
        &self,
        session_id: Option<&SessionId>,
        knowledge_graph: Option<String>,
        program: String,
        auth: Option<&crate::auth::Principal>,
        control: &Arc<RequestControl>,
    ) -> Result<QueryResult, ProgramError> {
        // Input size validation (protects parsing and downstream handlers)
        let max_bytes = self.config.storage.performance.max_query_size_bytes;
        if max_bytes > 0 && program.len() > max_bytes {
            return Err(format!(
                "Program too large: {} bytes (max {})",
                program.len(),
                max_bytes
            )
            .into());
        }

        let trimmed = program.trim();

        // Admission: one immutable permission snapshot for the whole program.
        let identity = auth
            .map(crate::auth::Principal::identity)
            .transpose()
            .map_err(String::from)?;
        let effective_auth = identity.as_ref();

        // Protect _internal KG from direct access.
        // Block both explicit commands AND sessions already bound to _internal.
        let session_kg_owned: Option<String> = if knowledge_graph.is_none() {
            match session_id {
                // A session whose KG binding cannot be read (reaped) must
                // not silently fall through to the storage default: the
                // per-KG ACL check keys off this value, and None skips it.
                Some(sid) => match self.sessions.session_kg(sid) {
                    Ok(kg) => Some(kg),
                    Err(_) if trimmed.starts_with(".ontology") => {
                        return Err(
                            "session expired - reconnect before running .ontology commands"
                                .to_string()
                                .into(),
                        );
                    }
                    Err(_) => None,
                },
                None => None,
            }
        } else {
            None
        };
        // The KG the program runs on: authorization and execution both use it.
        let exec_kg: Option<String> = knowledge_graph.clone().or(session_kg_owned).or_else(|| {
            self.storage
                .read()
                .current_knowledge_graph()
                .map(str::to_string)
        });
        let current_kg = exec_kg.as_deref();

        if let Some(identity) = effective_auth {
            if identity.role != crate::auth::Role::Admin
                && current_kg == Some(crate::auth::INTERNAL_KG)
            {
                return Err(internal_kg_denied().into());
            }
        }

        // Parsed once: these exact statements are authorized and executed.
        let statements = parse_program(&program).ok();
        let stmts = statements.as_deref().unwrap_or_default();
        self.authorize_program(effective_auth, current_kg, stmts)?;
        if stmts.len() > 1
            && stmts.iter().any(|stmt| {
                matches!(
                    stmt,
                    statement::Statement::Meta(MetaCommand::KgCreate(_) | MetaCommand::KgDrop(_))
                )
            })
        {
            return Err(
                "'.kg create' and '.kg drop' must be sent as a single statement"
                    .to_string()
                    .into(),
            );
        }
        // The single statement the intercepts below handle.
        let parsed = match stmts {
            [stmt] => Some(stmt),
            _ => None,
        };

        // Commands answered below, outside the program executor, change state
        // as they run: they enter the commit first, so a stop either prevents
        // them or cannot claim they did not happen.
        if parsed.is_some_and(|stmt| intercepted_mutation(stmt, session_id.is_some())) {
            control.begin_commit().map_err(supervise::stop_error)?;
        }

        // Any session-bound activity should keep the session alive.
        // If the session was reaped (e.g., after WS reconnect), log and continue
        // rather than failing the query - the session state is lost but queries
        // against persistent data should still work.
        if let Some(sid) = session_id {
            if let Err(e) = self.sessions.touch_session(sid) {
                tracing::debug!(session_id = %sid, error = %e, "session_touch_failed_continuing");
            }
        }

        // Fast path: intercept session meta commands that need SessionManager
        if trimmed.starts_with('.') {
            if let Some(statement::Statement::Meta(meta)) = parsed {
                match meta {
                    MetaCommand::SessionList => {
                        let sid = session_id.ok_or_else(|| "No active session".to_string())?;
                        return Ok(self.handle_session_list(sid)?);
                    }
                    MetaCommand::SessionClear => {
                        let sid = session_id.ok_or_else(|| "No active session".to_string())?;
                        // Get counts before clearing
                        let (facts_count, rules_count) =
                            self.sessions.with_session(sid, |session| {
                                let facts: usize =
                                    session.ephemeral_facts().values().map(Vec::len).sum();
                                (facts, session.rules().len())
                            })?;
                        self.sessions.clear_session(sid)?;
                        let msg = format!(
                            "Cleared {facts_count} session fact(s), {rules_count} session rule(s)."
                        );
                        return Ok(self.message_result(&msg));
                    }
                    MetaCommand::SessionDrop(index) => {
                        let sid = session_id.ok_or_else(|| "No active session".to_string())?;
                        return Ok(self.handle_session_drop(sid, *index)?);
                    }
                    MetaCommand::SessionDropName(name) => {
                        let sid = session_id.ok_or_else(|| "No active session".to_string())?;
                        return Ok(self.handle_session_drop_name(sid, name)?);
                    }

                    // Ontology lifecycle: async (registry fetch + recursive
                    // program execution), so it cannot run inside the sync
                    // executor. Engine-owned; il and the Studio delegate.
                    MetaCommand::OntologyInstall(spec) => {
                        let spec = spec.clone();
                        return self
                            .handle_ontology_install(session_id, knowledge_graph, &spec, auth)
                            .await;
                    }
                    MetaCommand::OntologyRemove(name) => {
                        let name = name.clone();
                        return self
                            .handle_ontology_remove(session_id, knowledge_graph, &name, auth)
                            .await;
                    }
                    MetaCommand::OntologyUpgrade(spec) => {
                        let spec = spec.clone();
                        return self
                            .handle_ontology_upgrade(session_id, knowledge_graph, &spec, auth)
                            .await;
                    }

                    // User & API key management (handled directly, not via query_program)
                    MetaCommand::UserList => {
                        return self.handle_user_list();
                    }
                    MetaCommand::UserCreate {
                        username,
                        password,
                        role,
                    } => {
                        return self.handle_user_create(username, password, role);
                    }
                    MetaCommand::UserDrop(username) => {
                        return self.handle_user_drop(username);
                    }
                    MetaCommand::UserPassword { username, password } => {
                        return self.handle_user_password(username, password);
                    }
                    MetaCommand::UserRole { username, role } => {
                        return self.handle_user_role(username, role);
                    }
                    MetaCommand::ApiKeyCreate { label, ttl, scope } => {
                        let owner = effective_auth
                            .map_or_else(|| "admin".to_string(), |a| a.username.clone());
                        return self.handle_apikey_create(label, &owner, *ttl, scope.clone());
                    }
                    MetaCommand::ApiKeyList => {
                        return Ok(self.handle_apikey_list());
                    }
                    MetaCommand::ApiKeyRevoke(label) => {
                        return self.handle_apikey_revoke(label);
                    }
                    MetaCommand::ApiKeyExpire { label, ttl } => {
                        return self.handle_apikey_expire(label, *ttl);
                    }

                    // KG ACL management
                    MetaCommand::KgAclList(ref kg_filter) => {
                        let effective_kg = kg_filter
                            .as_deref()
                            .or(current_kg)
                            .ok_or_else(|| "No knowledge graph selected".to_string())?;
                        return self
                            .handle_kg_acl_list(effective_kg)
                            .map(|msg| self.message_result(&msg));
                    }
                    MetaCommand::KgAclGrant {
                        ref kg_name,
                        ref username,
                        ref role,
                        ref relations,
                    } => {
                        return self
                            .grant_kg_access(kg_name, username, role, relations.as_deref())
                            .map(|msg| self.message_result(&msg));
                    }
                    MetaCommand::KgAclRevoke {
                        ref kg_name,
                        ref username,
                    } => {
                        return self
                            .handle_kg_acl_revoke(kg_name, username)
                            .map(|msg| self.message_result(&msg));
                    }

                    _ => {} // handled by query_program
                }
            }
        }

        // Intercept session rules and facts when session_id is present.
        // In the WS protocol each statement is a separate request, so we must
        // persist them in the SessionManager (not in a request-local vector).
        if let Some(sid) = session_id {
            if let Some(stmt) = parsed {
                match stmt {
                    statement::Statement::SessionRule(rule) => {
                        let existing_rules = self
                            .sessions
                            .with_session(sid, |session| session.rules().to_vec())?;
                        if let Err(err) = check_session_rule(
                            &self.get_storage(),
                            current_kg,
                            &existing_rules,
                            rule,
                        ) {
                            return Ok(Self::statement_failure(ErrorCode::Validation, err));
                        }

                        let rule_text = format_rule_text(rule);
                        self.sessions
                            .add_ephemeral_rule(sid, rule.clone(), rule_text)?;
                        return Ok(self.message_result(&format!(
                            "Session rule added for '{}'.",
                            rule.head.relation
                        )));
                    }
                    statement::Statement::Fact(rule) => {
                        let tuple = match session_fact_tuple(rule) {
                            Ok(tuple) => tuple,
                            Err(err) => {
                                return Ok(Self::statement_failure(ErrorCode::Validation, err))
                            }
                        };
                        let relation = rule.head.relation.clone();
                        self.sessions
                            .insert_ephemeral(sid, &relation, vec![tuple])?;
                        return Ok(self.message_result(&format!(
                            "Session fact added for '{relation}'. (Use +{relation}(...) to persist)"
                        )));
                    }
                    _ => {} // handled by query_program / query_program_with_session
                }
            }
        }

        // Only queries need session-aware execution (to prepend ephemeral rules).
        // All other statements (meta commands, inserts, deletes, persistent rules)
        // must go through query_program() directly because query_program_with_session()
        // prepends session rules and sends to the query engine, which breaks
        // non-query input (e.g., ".kg use default" is not a valid IQL atom).
        // Note: SessionRule and Fact are already intercepted above and stored
        // in the SessionManager, so they never reach this point.
        let is_query = trimmed.starts_with('?');

        // Detect KG create/drop before program is moved into query_program.
        // Extracting these from the parsed statement avoids fragile string matching
        // on the result messages.
        let (kg_create_name, kg_drop_name) = match parsed {
            Some(statement::Statement::Meta(statement::MetaCommand::KgCreate(name))) => {
                (Some(name.clone()), None)
            }
            Some(statement::Statement::Meta(statement::MetaCommand::KgDrop(name))) => {
                (None, Some(name.clone()))
            }
            _ => (None, None),
        };

        // A non-query on a reaped session errors instead of running on the
        // storage default.
        if let (Some(sid), None, false) = (session_id, &knowledge_graph, is_query) {
            self.sessions.session_kg(sid)?;
        }
        let mut result = match session_id {
            Some(sid) if is_query => {
                self.run_program_with_session(sid, exec_kg, program, statements, control)
                    .await?
            }
            _ => {
                self.run_program(exec_kg, program, statements, control)
                    .await?
            }
        };

        // If KG was switched, update session binding
        if let (Some(ref new_kg), Some(sid)) = (&result.switched_kg, session_id) {
            self.sessions.switch_kg(sid, new_kg)?;
        }

        // Auto-grant owner ACL to the creator of a new KG.
        // Verified by checking switched_kg (only set on successful create).
        if let Some(identity) = effective_auth {
            if identity.role != crate::auth::Role::Admin {
                if let Some(ref name) = kg_create_name {
                    if result.switched_kg.as_deref() == Some(name.as_str()) {
                        if let Err(error) =
                            self.handle_kg_acl_grant(name, &identity.username, "owner")
                        {
                            result.errors.push(StatementError {
                                index: 0,
                                code: error.code.unwrap_or(ErrorCode::Internal),
                                message: error.message,
                            });
                        }
                    }
                }
            }
        }

        // If a KG was dropped, clean up sessions and ACLs.
        if let Some(ref name) = kg_drop_name {
            if result.errors.is_empty() {
                self.sessions.close_sessions_for_kg(name);
                if let Err(error) = self.cleanup_kg_acls(name) {
                    if matches!(
                        error.code,
                        Some(ErrorCode::OutcomeUnknown | ErrorCode::StoreReadOnly)
                    ) {
                        return Err(error);
                    }
                    warn!(kg = %name, error = %error, "kg_drop_acl_cleanup_failed");
                    result.rows.push(WireTuple {
                        values: vec![WireValue::String(format!(
                            "Access entries for '{name}' were not removed: {error}"
                        ))],
                        provenance: None,
                    });
                    result.total_count = result.rows.len();
                }
            }
        }

        Ok(result)
    }

    /// Authorize every statement against the KG in effect at that statement.
    ///
    /// A failed `.kg use` leaves the previous KG active, so statements after a
    /// switch are checked against every KG that may be current. With no
    /// statements (unparseable program), the caller still needs a role on the
    /// current KG.
    fn authorize_program(
        &self,
        auth: Option<&crate::auth::AuthIdentity>,
        current_kg: Option<&str>,
        statements: &[statement::Statement],
    ) -> Result<(), String> {
        use crate::auth::{self, Role, INTERNAL_KG};
        use statement::Statement;

        let mut kgs: Vec<String> = current_kg.map(str::to_string).into_iter().collect();
        let non_admin = auth.filter(|identity| identity.role != Role::Admin);
        let kg_role = |kg: &str, identity: &auth::AuthIdentity| {
            if kg == INTERNAL_KG {
                return Err(internal_kg_denied());
            }
            self.kg_access(kg, identity)
                .ok_or_else(|| match &identity.key_scope {
                    Some(key) if key.scope.kg != kg => format!(
                        "Access denied: this API key is scoped to knowledge graph '{}'",
                        key.scope.kg
                    ),
                    _ => "Access denied".to_string(),
                })
        };

        if statements.is_empty() {
            if let Some(identity) = non_admin {
                for kg in &kgs {
                    kg_role(kg, identity)?;
                }
            }
        }

        for stmt in statements {
            if let Some(identity) = non_admin {
                auth::authorize_statement(&identity.role, stmt)?;
                if let (Some(key), Statement::Meta(MetaCommand::KgCreate(_))) =
                    (&identity.key_scope, stmt)
                {
                    return Err(format!(
                        "Permission denied: this API key is scoped to knowledge graph '{}' \
                         and cannot create one",
                        key.scope.kg
                    ));
                }
            }
            if targets_internal_kg(stmt) {
                return Err(internal_kg_denied());
            }
            if let Some(identity) = non_admin {
                let targets: Vec<&str> = match stmt {
                    Statement::Meta(
                        MetaCommand::KgDrop(name)
                        | MetaCommand::KgUse(name)
                        | MetaCommand::KgAclList(Some(name)),
                    ) => vec![name.as_str()],
                    Statement::Meta(
                        MetaCommand::KgAclGrant { kg_name, .. }
                        | MetaCommand::KgAclRevoke { kg_name, .. },
                    ) => vec![kg_name.as_str()],
                    // Create targets no existing KG; list/show/help are global.
                    Statement::Meta(
                        MetaCommand::KgCreate(_)
                        | MetaCommand::KgList
                        | MetaCommand::KgShow
                        | MetaCommand::Help
                        | MetaCommand::Quit
                        | MetaCommand::Status,
                    ) => Vec::new(),
                    _ => kgs.iter().map(String::as_str).collect(),
                };
                for kg in targets {
                    auth::authorize_kg_operation(&kg_role(kg, identity)?, kg, stmt)?;
                }
            }
            if let Statement::Meta(MetaCommand::KgUse(name) | MetaCommand::KgCreate(name)) = stmt {
                if !kgs.contains(name) {
                    kgs.push(name.clone());
                }
            }
        }
        Ok(())
    }

    /// Build a single-message QueryResult
    fn message_result(&self, msg: &str) -> QueryResult {
        QueryResult {
            rows: vec![WireTuple {
                values: vec![WireValue::String(msg.to_string())],
                provenance: None,
            }],
            schema: vec![ColumnDef {
                name: "message".to_string(),
                data_type: WireDataType::String,
            }],
            total_count: 1,
            truncated: false,
            execution_time_ms: 0,
            metadata: None,
            switched_kg: None,
            proof_trees: None,
            timing_breakdown: None,
            errors: Vec::new(),
            statements: Vec::new(),
            revision: None,
        }
    }

    /// The registry the engine loads ontologies from: `INPUTLAYER_REGISTRY`
    /// (HTTPS index URL, or a local filesystem path to a cloned registry for
    /// deployments with no outbound network), token from
    /// `INPUTLAYER_REGISTRY_TOKEN` / `GITHUB_TOKEN`.
    fn ontology_registry() -> inputlayer_ontology_client::registry::Registry {
        let index = std::env::var("INPUTLAYER_REGISTRY")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| inputlayer_ontology_client::registry::DEFAULT_INDEX_URL.to_string());
        let token = std::env::var("INPUTLAYER_REGISTRY_TOKEN")
            .ok()
            .filter(|t| !t.trim().is_empty())
            .or_else(|| std::env::var("GITHUB_TOKEN").ok())
            .filter(|t| !t.trim().is_empty());
        inputlayer_ontology_client::registry::Registry::new(index, token)
    }

    /// Current KG for an `.ontology` command: explicit param, else the
    /// session's bound KG, else the storage default.
    fn resolve_ontology_kg(
        &self,
        session_id: Option<&SessionId>,
        knowledge_graph: Option<&String>,
    ) -> Result<String, String> {
        if let Some(kg) = knowledge_graph {
            return Ok(kg.clone());
        }
        if let Some(sid) = session_id {
            if let Ok(kg) = self.sessions.session_kg(sid) {
                return Ok(kg);
            }
        }
        let storage = self.storage.read();
        storage
            .current_knowledge_graph()
            .map(str::to_string)
            .ok_or_else(|| "No knowledge graph selected".to_string())
    }

    /// Messages of the statements that failed in `result`.
    fn result_problem_rows(result: &QueryResult) -> Result<Vec<String>, ProgramError> {
        if let Some(error) = result
            .errors
            .iter()
            .find(|e| matches!(e.code, ErrorCode::OutcomeUnknown | ErrorCode::StoreReadOnly))
        {
            return Err(ProgramError {
                code: Some(error.code),
                message: error.message.clone(),
            });
        }
        Ok(result.errors.iter().map(|e| e.message.clone()).collect())
    }

    /// The result of a one-statement program whose statement failed.
    fn statement_failure(code: ErrorCode, message: String) -> QueryResult {
        QueryResult {
            errors: vec![StatementError {
                index: 0,
                code,
                message: message.clone(),
            }],
            ..Self::messages_result(vec![message])
        }
    }

    fn messages_result(messages: Vec<String>) -> QueryResult {
        let rows: Vec<WireTuple> = messages
            .into_iter()
            .map(|msg| WireTuple {
                values: vec![WireValue::String(msg)],
                provenance: None,
            })
            .collect();
        let total_count = rows.len();
        QueryResult {
            rows,
            schema: vec![ColumnDef {
                name: "message".to_string(),
                data_type: WireDataType::String,
            }],
            total_count,
            truncated: false,
            execution_time_ms: 0,
            metadata: None,
            switched_kg: None,
            proof_trees: None,
            timing_breakdown: None,
            errors: Vec::new(),
            statements: Vec::new(),
            revision: None,
        }
    }

    /// Existing `pack_item` rows for a pack (kind, item), empty when the
    /// relation does not exist yet.
    async fn pack_items(
        &self,
        session_id: Option<&SessionId>,
        kg: &str,
        name: &str,
        auth: Option<&crate::auth::Principal>,
    ) -> Vec<(String, String)> {
        let query = "?pack_item(P, K, I)".to_string();
        let result =
            Box::pin(self.execute_program(session_id, Some(kg.to_string()), query, auth)).await;
        let Ok(result) = result else {
            return Vec::new();
        };
        result
            .rows
            .iter()
            .filter_map(|row| {
                let get = |i: usize| match row.values.get(i) {
                    Some(WireValue::String(s)) => Some(s.clone()),
                    _ => None,
                };
                match (get(0), get(1), get(2)) {
                    (Some(p), Some(k), Some(i)) if p == name => Some((k, i)),
                    _ => None,
                }
            })
            .collect()
    }

    /// Inventory items owned by packs OTHER than `name` in this KG.
    async fn pack_items_of_others(
        &self,
        session_id: Option<&SessionId>,
        kg: &str,
        name: &str,
        auth: Option<&crate::auth::Principal>,
    ) -> std::collections::BTreeSet<(String, String)> {
        let result = Box::pin(self.execute_program(
            session_id,
            Some(kg.to_string()),
            "?pack_item(P, K, I)".to_string(),
            auth,
        ))
        .await;
        let Ok(result) = result else {
            return std::collections::BTreeSet::new();
        };
        result
            .rows
            .iter()
            .filter_map(|row| {
                let get = |i: usize| match row.values.get(i) {
                    Some(WireValue::String(s)) => Some(s.clone()),
                    _ => None,
                };
                match (get(0), get(1), get(2)) {
                    (Some(p), Some(k), Some(i)) if p != name => Some((k, i)),
                    _ => None,
                }
            })
            .collect()
    }

    /// The pinned (version, digest) for a pack in a KG, if any.
    async fn pack_pin(
        &self,
        session_id: Option<&SessionId>,
        kg: &str,
        name: &str,
        auth: Option<&crate::auth::Principal>,
    ) -> Option<(String, String)> {
        let result = Box::pin(self.execute_program(
            session_id,
            Some(kg.to_string()),
            "?pack_meta(N, V, D)".to_string(),
            auth,
        ))
        .await
        .ok()?;
        result.rows.iter().find_map(|row| {
            let get = |i: usize| match row.values.get(i) {
                Some(WireValue::String(s)) => Some(s.clone()),
                _ => None,
            };
            match (get(0), get(1), get(2)) {
                (Some(n), Some(v), Some(d)) if n == name => Some((v, d)),
                _ => None,
            }
        })
    }

    /// `.ontology install <name[@version]>`: fetch the pack from the
    /// registry (digest-verified), deploy its rules into the current KG, pin
    /// it in `pack_meta`, and record the deployed rule/relation inventory in
    /// `pack_item` so remove/upgrade are exact.
    async fn handle_ontology_install(
        &self,
        session_id: Option<&SessionId>,
        knowledge_graph: Option<String>,
        spec: &str,
        auth: Option<&crate::auth::Principal>,
    ) -> Result<QueryResult, ProgramError> {
        use inputlayer_ontology_client::registry;
        let kg = self.resolve_ontology_kg(session_id, knowledge_graph.as_ref())?;
        let reg = Self::ontology_registry();
        let (name, entry) = reg
            .resolve(spec)
            .await
            .map_err(|e| format!("ontology resolve failed: {e:#}"))?;
        let entry_dir = reg
            .fetch(&name, &entry)
            .await
            .map_err(|e| format!("ontology fetch failed: {e:#}"))?;
        let manifest =
            registry::read_manifest(&entry_dir).map_err(|e| format!("invalid pack: {e:#}"))?;
        let mut program = String::new();
        for rel in &manifest.ontology.rules {
            let path = entry_dir.join(rel);
            let text = std::fs::read_to_string(&path)
                .map_err(|e| format!("cannot read pack rules file {rel}: {e}"))?;
            program.push_str(&text);
            program.push('\n');
        }
        let statement_count = program
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with("//"))
            .count();

        // A pack is DATA, not a script. Digest pinning proves the pack is
        // what the index said - not that it is safe. Rules programs execute
        // as multi-statement programs, which bypasses per-statement and
        // per-KG authorization, so anything other than schema/rule/fact
        // statements is refused before deployment: no meta commands (a
        // `.kg use other-kg` would write into another KG, a nested
        // `.ontology install` would recurse without bound), no deletes.
        for line in join_continuation_lines(&strip_comments(&program)).lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            // Bookkeeping relations are the engine's, not a pack's: a pack
            // that could insert into pack_meta would forge ANOTHER pack's
            // pin, and a gateway trusting that pin would report "verified"
            // over rules that were never deployed.
            const RESERVED: [&str; 3] = ["pack_meta", "pack_item", "il_conversation"];
            let reserved_target = match &statement::parse_statement(line) {
                Ok(statement::Statement::Insert(op)) => RESERVED.contains(&op.relation.as_str()),
                Ok(statement::Statement::SchemaDecl(decl)) => {
                    RESERVED.contains(&decl.name.as_str())
                }
                Ok(statement::Statement::PersistentRule(rule)) => {
                    RESERVED.contains(&rule.head.relation.as_str())
                }
                _ => false,
            };
            if reserved_target {
                return Err(format!(
                    "pack {name} writes a reserved relation (pack_meta, pack_item, \
                     and il_conversation belong to the engine)"
                )
                .into());
            }
            match statement::parse_statement(line) {
                Ok(
                    statement::Statement::SchemaDecl(_)
                    | statement::Statement::PersistentRule(_)
                    | statement::Statement::Insert(_)
                    | statement::Statement::TypeDecl(_),
                ) => {}
                // Facts (`foo("a").`) become SESSION facts and are silently
                // discarded, so a pack shipping them would report installed
                // statements that do not exist afterwards.
                Ok(statement::Statement::Fact(_)) => {
                    return Err(format!(
                        "pack {name} uses a session fact (`rel(...).`); packs must use \
                         persistent inserts (`+rel[(...)]`)"
                    )
                    .into());
                }
                Ok(other) => {
                    return Err(format!(
                        "pack {name} contains a disallowed statement (packs may only declare \
                         schemas, rules, types, and facts): {}",
                        match other {
                            statement::Statement::Meta(_) => "meta command",
                            statement::Statement::Delete(_) => "delete",
                            statement::Statement::Update(_) => "update",
                            statement::Statement::Query(_) => "query",
                            statement::Statement::SessionRule(_) => "session rule",
                            statement::Statement::DeleteRelationOrRule(_) => "drop",
                            _ => "unsupported statement",
                        }
                    )
                    .into());
                }
                Err(err) => {
                    return Err(format!("pack {name} has an unparsable statement: {err}").into());
                }
            }
        }

        // Inventory before, so the diff identifies what this pack added.
        let (rules_before, rels_before) = {
            let storage = self.storage.read();
            (
                storage.list_rules_in(&kg).map_err(|e| e.to_string())?,
                storage.list_relations_in(&kg).map_err(|e| e.to_string())?,
            )
        };
        let prior_items = self.pack_items(session_id, &kg, &name, auth).await;
        // Installing a DIFFERENT version over an existing one would stack
        // both rule sets while pack_meta claims only the new one - findings
        // attributed to rules that are not the ones that derived them.
        if let Some((pinned_version, _)) = self.pack_pin(session_id, &kg, &name, auth).await {
            if pinned_version != entry.version {
                return Err(format!(
                    "ontology '{name}' is already installed in '{kg}' at {pinned_version}; \
                     use .ontology upgrade {name}@{} to replace it",
                    entry.version
                )
                .into());
            }
        }
        let program_copy = program.clone();

        let deploy = Box::pin(self.run_execute_program(
            session_id,
            Some(kg.clone()),
            program,
            auth,
            &self.request_control(None),
        ))
        .await?;
        let problems = Self::result_problem_rows(&deploy)?;
        if !problems.is_empty() {
            // The deploy program is one transaction: none of it was applied.
            return Err(format!(
                "pack deployment failed; nothing was applied: {}",
                problems.join("; ")
            )
            .into());
        }

        let (rules_after, rels_after) = {
            let storage = self.storage.read();
            (
                storage.list_rules_in(&kg).map_err(|e| e.to_string())?,
                storage.list_relations_in(&kg).map_err(|e| e.to_string())?,
            )
        };
        // Inventory = what the pack program itself declares (parsed), plus
        // the deploy diff, plus any previously recorded inventory. Parsing
        // is the primary source: a declared-but-empty relation does not
        // appear in the relation listing until its first insert, so a diff
        // alone would miss exactly the pack's data relations.
        let mut items: std::collections::BTreeSet<(String, String)> =
            prior_items.into_iter().collect();
        for line in join_continuation_lines(&strip_comments(&program_copy)).lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match statement::parse_statement(line) {
                Ok(statement::Statement::SchemaDecl(decl)) if decl.persistent => {
                    items.insert(("relation".to_string(), decl.name));
                }
                Ok(statement::Statement::PersistentRule(rule)) => {
                    items.insert(("rule".to_string(), rule.head.relation));
                }
                _ => {}
            }
        }
        for rule in &rules_after {
            if !rules_before.contains(rule) {
                items.insert(("rule".to_string(), rule.clone()));
            }
        }
        for rel in &rels_after {
            if !rels_before.contains(rel) {
                items.insert(("relation".to_string(), rel.clone()));
            }
        }

        // Bookkeeping relations; the decls may already exist.
        let declared = Box::pin(
            self.run_execute_program(
                session_id,
                Some(kg.clone()),
                "+pack_meta(name: string, version: string, digest: string)\n\
             +pack_item(pack: string, kind: string, item: string)"
                    .to_string(),
                auth,
                &self.request_control(None),
            ),
        )
        .await?;
        Self::result_problem_rows(&declared)?;
        // Replace any previous pin/inventory rows for this pack, then record.
        let mut record = String::new();
        record.push_str(&format!(
            "-pack_meta(N, V, D) <- pack_meta(N, V, D), N = \"{name}\"\n"
        ));
        record.push_str(&format!(
            "-pack_item(P, K, I) <- pack_item(P, K, I), P = \"{name}\"\n"
        ));
        record.push_str(&format!(
            "+pack_meta[(\"{}\", \"{}\", \"{}\")]\n",
            name, entry.version, entry.digest
        ));
        for (kind, item) in &items {
            record.push_str(&format!(
                "+pack_item[(\"{name}\", \"{kind}\", \"{item}\")]\n"
            ));
        }
        let recorded = Box::pin(self.run_execute_program(
            session_id,
            Some(kg.clone()),
            record,
            auth,
            &self.request_control(None),
        ))
        .await?;
        let record_problems = Self::result_problem_rows(&recorded)?;
        if !record_problems.is_empty() {
            return Err(format!(
                "pack deployed but pin/inventory recording failed: {}",
                record_problems.join("; ")
            )
            .into());
        }

        Ok(Self::messages_result(vec![
            format!(
                "installed {}@{} into {kg} ({statement_count} statements)",
                name, entry.version
            ),
            format!("digest {}", entry.digest),
            format!(
                "recorded {} rule(s), {} relation(s) in pack_item",
                items.iter().filter(|(k, _)| k == "rule").count(),
                items.iter().filter(|(k, _)| k == "relation").count()
            ),
        ]))
    }

    /// `.ontology remove <name>`: drop the pack's recorded rules and
    /// relations (relations cascade their data) and clear the pins.
    async fn handle_ontology_remove(
        &self,
        session_id: Option<&SessionId>,
        knowledge_graph: Option<String>,
        name: &str,
        auth: Option<&crate::auth::Principal>,
    ) -> Result<QueryResult, ProgramError> {
        inputlayer_ontology_client::registry::validate_component("ontology name", name)
            .map_err(|e| e.to_string())?;
        let kg = self.resolve_ontology_kg(session_id, knowledge_graph.as_ref())?;
        let items = self.pack_items(session_id, &kg, name, auth).await;
        if items.is_empty() {
            return Err(format!(
                "ontology '{name}' is not installed in '{kg}' (no pack_item inventory)"
            )
            .into());
        }
        // Rules and relations another installed pack also declares are
        // SHARED: dropping them would delete the other pack's data and
        // leave its rules dangling. Leave them in place and report.
        let shared = self.pack_items_of_others(session_id, &kg, name, auth).await;
        let (items, retained): (Vec<(String, String)>, Vec<(String, String)>) =
            items.into_iter().partition(|item| !shared.contains(item));
        if items.is_empty() {
            return Err(format!(
                "every item of '{name}' in '{kg}' is shared with another installed pack; \
                 nothing to remove safely"
            )
            .into());
        }
        // Rules first (they depend on the relations), as one transaction of
        // the rules still present; then each relation on its own, since
        // `.rel drop` cannot join a transaction (this deletes the data they
        // hold - removal is destructive by design).
        let live_rules = self
            .storage
            .read()
            .list_rules_in(&kg)
            .map_err(|e| e.to_string())?;
        let mut programs: Vec<String> = Vec::new();
        let rule_drops = items
            .iter()
            .filter(|(kind, item)| kind == "rule" && live_rules.contains(item))
            .map(|(_, item)| format!(".rule drop {item}"))
            .collect::<Vec<_>>()
            .join("\n");
        if !rule_drops.is_empty() {
            programs.push(rule_drops);
        }
        programs.extend(
            items
                .iter()
                .filter(|(kind, _)| kind == "relation")
                .map(|(_, item)| format!(".rel drop {item}")),
        );
        let mut messages = vec![format!(
            "removed {name} from {kg} ({} rule(s), {} relation(s))",
            items.iter().filter(|(k, _)| k == "rule").count(),
            items.iter().filter(|(k, _)| k == "relation").count()
        )];
        // Failed drops are caught by the read-back below. Surface every
        // sub-result row: drops that fail phrase their errors in many ways,
        // and silence here would misreport a partial removal.
        for program in programs {
            let result = Box::pin(self.run_execute_program(
                session_id,
                Some(kg.clone()),
                program,
                auth,
                &self.request_control(None),
            ))
            .await?;
            Self::result_problem_rows(&result)?;
            for row in &result.rows {
                if let Some(WireValue::String(s)) = row.values.first() {
                    messages.push(format!("  {s}"));
                }
            }
        }
        // Trust the read-back, not the drop messages: verify the pack's
        // relations are actually gone.
        let leftovers: Vec<String> = {
            let storage = self.storage.read();
            let relations = storage.list_relations_in(&kg).map_err(|e| e.to_string())?;
            let rules = storage.list_rules_in(&kg).map_err(|e| e.to_string())?;
            items
                .iter()
                .filter(|(kind, item)| match kind.as_str() {
                    "relation" => relations.contains(item),
                    // Rules are verified too: a `.rule drop` that failed
                    // would otherwise leave live rules behind while the pin
                    // and inventory are cleared, making them untrackable.
                    "rule" => rules.contains(item),
                    _ => false,
                })
                .map(|(kind, item)| format!("{kind} {item}"))
                .collect()
        };
        if !leftovers.is_empty() {
            // Pin and inventory are still intact, so the pack stays
            // manageable (retry, or inspect) instead of becoming an
            // untracked orphan.
            return Err(format!(
                "removal incomplete: still present after drop: {} \
                 (pack_meta and pack_item left in place - retry or inspect the KG)",
                leftovers.join(", ")
            )
            .into());
        }
        // Only now that the drops are verified: clear the pin and inventory.
        let cleanup = Box::pin(self.run_execute_program(
            session_id,
            Some(kg.clone()),
            format!(
                "-pack_item(P, K, I) <- pack_item(P, K, I), P = \"{name}\"\n\
                 -pack_meta(N, V, D) <- pack_meta(N, V, D), N = \"{name}\""
            ),
            auth,
            &self.request_control(None),
        ))
        .await?;
        for problem in Self::result_problem_rows(&cleanup)? {
            messages.push(format!("  warning: {problem}"));
        }
        if !retained.is_empty() {
            messages.push(format!(
                "  kept {} item(s) shared with another installed pack",
                retained.len()
            ));
        }
        Ok(Self::messages_result(messages))
    }

    /// `.ontology upgrade <name[@version]>`: re-deploy the pack's rules at
    /// the new version while KEEPING the relations and their data (that is
    /// the point of an upgrade - conversations and facts survive).
    async fn handle_ontology_upgrade(
        &self,
        session_id: Option<&SessionId>,
        knowledge_graph: Option<String>,
        spec: &str,
        auth: Option<&crate::auth::Principal>,
    ) -> Result<QueryResult, ProgramError> {
        let name = spec.split('@').next().unwrap_or(spec).to_string();
        inputlayer_ontology_client::registry::validate_component("ontology name", &name)
            .map_err(|e| e.to_string())?;
        let kg = self.resolve_ontology_kg(session_id, knowledge_graph.as_ref())?;
        let items = self.pack_items(session_id, &kg, &name, auth).await;
        if items.is_empty() {
            return Err(format!(
                "ontology '{name}' is not installed in '{kg}' - use .ontology install"
            )
            .into());
        }
        let rule_items: Vec<&(String, String)> =
            items.iter().filter(|(k, _)| k == "rule").collect();
        // A rule that is already gone needs no dropping; dropping it would
        // fail the whole program.
        let live_rules = self
            .storage
            .read()
            .list_rules_in(&kg)
            .map_err(|e| e.to_string())?;
        let mut program = String::new();
        // The pin goes FIRST: between dropping the old rules and deploying
        // the new ones this KG runs no rule set, and a stale pin would tell
        // the gateway it is safe to evaluate against rules that are not
        // there. Unpinned means the gateway refuses - fail closed.
        program.push_str(&format!(
            "-pack_meta(N, V, D) <- pack_meta(N, V, D), N = \"{name}\"\n"
        ));
        for (_, item) in rule_items
            .iter()
            .filter(|(_, item)| live_rules.contains(item))
        {
            program.push_str(&format!(".rule drop {item}\n"));
        }
        // Drop only the RULE inventory rows; relation rows stay attributed.
        program.push_str(&format!(
            "-pack_item(P, K, I) <- pack_item(P, K, I), P = \"{name}\", K = \"rule\"\n"
        ));
        if !program.is_empty() {
            let result = Box::pin(self.run_execute_program(
                session_id,
                Some(kg.clone()),
                program,
                auth,
                &self.request_control(None),
            ))
            .await?;
            let problems = Self::result_problem_rows(&result)?;
            if !problems.is_empty() {
                return Err(format!(
                    "upgrade aborted while dropping old rules: {}",
                    problems.join("; ")
                )
                .into());
            }
        }
        let install = self
            .handle_ontology_install(session_id, Some(kg.clone()), spec, auth)
            .await
            .map_err(|err| ProgramError {
                code: err.code,
                message: format!(
                    "upgrade of '{name}' in '{kg}' left the KG WITHOUT rules and unpinned \
                     (evaluation refuses until a successful install): {err}"
                ),
            })?;
        let mut messages = vec![format!(
            "upgraded {name} in {kg} (dropped {} old rule(s), data kept)",
            rule_items.len()
        )];
        for row in &install.rows {
            if let Some(WireValue::String(s)) = row.values.first() {
                messages.push(s.clone());
            }
        }
        Ok(Self::messages_result(messages))
    }

    /// Handle `.session` list command
    fn handle_session_list(&self, session_id: &SessionId) -> Result<QueryResult, String> {
        let mut messages = Vec::new();
        self.sessions.with_session(session_id, |session| {
            let has_facts = !session.ephemeral_facts().is_empty();
            let has_rules = !session.rules().is_empty();

            if !has_facts && !has_rules {
                messages.push("No session data defined.".to_string());
            } else {
                if has_facts {
                    let count: usize = session.ephemeral_facts().values().map(Vec::len).sum();
                    messages.push(format!("Session facts ({count}):"));
                    let mut relations: Vec<&String> = session.ephemeral_facts().keys().collect();
                    relations.sort();
                    for rel in relations {
                        if let Some(tuples) = session.ephemeral_facts().get(rel) {
                            for tuple in tuples {
                                messages.push(format!("  {rel}({tuple})"));
                            }
                        }
                    }
                }
                if has_rules {
                    messages.push(format!("Session rules ({}):", session.rules().len()));
                    for (i, rule) in session.rules().iter().enumerate() {
                        messages.push(format!("  {}. {rule}", i + 1));
                    }
                }
            }
        })?;

        let rows: Vec<WireTuple> = messages
            .iter()
            .map(|msg| WireTuple {
                values: vec![WireValue::String(msg.clone())],
                provenance: None,
            })
            .collect();
        let total_count = rows.len();
        Ok(QueryResult {
            rows,
            schema: vec![ColumnDef {
                name: "message".to_string(),
                data_type: WireDataType::String,
            }],
            total_count,
            truncated: false,
            execution_time_ms: 0,
            metadata: None,
            switched_kg: None,
            proof_trees: None,
            timing_breakdown: None,
            errors: Vec::new(),
            statements: Vec::new(),
            revision: None,
        })
    }

    /// Handle `.session drop <index>` command
    fn handle_session_drop(
        &self,
        session_id: &SessionId,
        index: usize,
    ) -> Result<QueryResult, String> {
        let inner_result: Result<String, String> =
            self.sessions.with_session_mut(session_id, |session| {
                if index >= session.rules().len() {
                    Err(format!("Rule index {} out of bounds.", index + 1))
                } else {
                    let removed = session.rules()[index].clone();
                    session.remove_ephemeral_rule(index);
                    Ok(format!("Removed rule {}: {removed}", index + 1))
                }
            })?;
        let msg = inner_result?;
        Ok(self.message_result(&msg))
    }

    /// Handle `.session drop <name>` command
    fn handle_session_drop_name(
        &self,
        session_id: &SessionId,
        name: &str,
    ) -> Result<QueryResult, String> {
        let msg = self.sessions.with_session_mut(session_id, |session| {
            let before = session.rules().len();
            session.remove_ephemeral_rules_by_name(name);
            let removed = before - session.rules().len();
            if removed == 0 {
                format!("No session rules found for relation '{name}'.")
            } else {
                format!("Dropped {removed} session rule(s) for '{name}'.")
            }
        })?;
        Ok(self.message_result(&msg))
    }
}

// Helper Functions

/// Transform `?shorthand` query syntax into a `__query__(...) <- ...` rule.
///
/// This enables the shorthand `?relation(X, Y)` syntax that the REPL and
/// WebSocket API use, converting it to a proper IQL rule before execution.
/// Returns the original text unchanged if it's not a `?shorthand` query.
///
/// Also extracts `:asc`/`:desc` sort annotations from query head variables,
/// e.g. `?rel(X, Score:desc)` → sort by column 1 descending.
pub(crate) fn transform_query_shorthand(program_text: &str) -> Result<QueryTransform, String> {
    let trimmed = program_text.trim();
    if let Some(after_q) = trimmed.strip_prefix('?') {
        let after_q = after_q.trim_start();
        if !after_q.starts_with(|c: char| c.is_alphabetic() || c == '_') {
            return Ok(QueryTransform {
                query: program_text.to_string(),
                order_by: vec![],
                limit: None,
                offset: None,
                columns: vec![],
            });
        }
        let query_text = after_q;
        let goal = statement::parse_query(query_text)
            .map_err(|e| format!("Failed to parse query: {e}"))?;

        let mut head_vars = Vec::new();
        let mut extra_constraints = Vec::new();

        let transformed_args: Vec<String> = goal
            .goal
            .iter()
            .flat_map(|g| g.args.iter())
            .enumerate()
            .map(|(i, term)| match term {
                Term::Variable(v) => {
                    head_vars.push(v.clone());
                    v.clone()
                }
                Term::Constant(_)
                | Term::FloatConstant(_)
                | Term::BoolConstant(_)
                | Term::StringConstant(_) => {
                    let t = format!("_c{i}");
                    head_vars.push(t.clone());
                    extra_constraints.push(format!("{t} = {term}"));
                    t
                }
                Term::VectorLiteral(_) => {
                    // Vector literals can't be used in comparison constraints
                    // (parser doesn't support [1,2,3] in comparison context).
                    // Use a fresh variable - returns all rows for this position.
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

        let mut body_parts: Vec<String> = goal
            .goal
            .iter()
            .map(|g| format!("{}({})", g.relation, transformed_args.join(", ")))
            .collect();

        for pred in &goal.body {
            body_parts.push(format_body_pred(pred));
            extract_predicate_vars(pred, &mut head_vars);
        }

        body_parts.extend(extra_constraints);

        // Map sort annotations (variable names) to column indices in head_vars
        let order_by: Vec<(usize, SortDirection)> = goal
            .order_by
            .iter()
            .filter_map(|(var_name, dir)| {
                head_vars
                    .iter()
                    .position(|v| v == var_name)
                    .map(|idx| (idx, *dir))
            })
            .collect();

        Ok(QueryTransform {
            query: format!(
                "__query__({}) <- {}",
                head_vars.join(", "),
                body_parts.join(", ")
            ),
            order_by,
            limit: goal.limit,
            offset: goal.offset,
            columns: head_vars,
        })
    } else {
        Ok(QueryTransform {
            query: program_text.to_string(),
            order_by: vec![],
            limit: None,
            offset: None,
            columns: vec![],
        })
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

/// Join continuation lines in a program.
///
/// A continuation line starts with whitespace (spaces/tabs) and is appended
/// to the previous non-empty line. This supports multi-line rules like:
///
/// ```text
/// reachable(X, Y) <-
///   edge(X, Y).
/// ```
///
/// which becomes: `reachable(X, Y) <- edge(X, Y).`
///
/// Lines starting at column 0 are treated as new statements and are never
/// merged with the previous line.
fn join_continuation_lines(program: &str) -> String {
    let mut result: Vec<String> = Vec::new();
    for line in program.lines() {
        if line.trim().is_empty() {
            result.push(String::new());
            continue;
        }
        // Continuation: non-empty line that starts with whitespace
        if line.starts_with(|c: char| c.is_whitespace()) && !result.is_empty() {
            // Find the last non-empty line to append to
            if let Some(last) = result.iter_mut().rev().find(|l| !l.is_empty()) {
                last.push(' ');
                last.push_str(line.trim());
                continue;
            }
        }
        result.push(line.to_string());
    }
    result.join("\n")
}

/// Split a program into statements the way `QueryJob` executes it: comments
/// stripped, continuation lines joined, one statement per non-empty line.
fn parse_program(program: &str) -> Result<Vec<statement::Statement>, Vec<ValidationError>> {
    let mut statements = Vec::new();
    let mut errors = Vec::new();
    let text = join_continuation_lines(&strip_comments(program));
    for (line_num, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match statement::parse_statement(line) {
            Ok(stmt) => statements.push(stmt),
            Err(error) => errors.push(ValidationError {
                line: line_num + 1,
                statement_index: statements.len() + errors.len(),
                error,
            }),
        }
    }
    if errors.is_empty() {
        Ok(statements)
    } else {
        Err(errors)
    }
}

/// Whether every statement of `program`, split the way it executes, is a
/// query (`?...`): such a program reads its KG and session without changing
/// either. Lexical, so it costs no parse: a `?` statement parses as a query
/// or fails without effect.
pub(crate) fn is_query_program(program: &str) -> bool {
    let text = join_continuation_lines(&strip_comments(program));
    let mut statements = text.lines().map(str::trim).filter(|line| !line.is_empty());
    let mut any = false;
    statements.all(|line| {
        any = true;
        line.starts_with('?')
    }) && any
}

/// Whether a statement creates, switches to or drops the system KG.
fn targets_internal_kg(stmt: &statement::Statement) -> bool {
    matches!(
        stmt,
        statement::Statement::Meta(
            MetaCommand::KgUse(name) | MetaCommand::KgDrop(name) | MetaCommand::KgCreate(name),
        ) if name == crate::auth::INTERNAL_KG
    )
}

/// `code` for a storage error, `default` when its variant does not say.
fn storage_error_code(error: &crate::storage::StorageError, default: ErrorCode) -> ErrorCode {
    use crate::storage::StorageError;
    match error {
        StorageError::OutcomeUnknown { .. } => ErrorCode::OutcomeUnknown,
        StorageError::StoreReadOnly => ErrorCode::StoreReadOnly,
        StorageError::MemoryBudgetExceeded { .. } => ErrorCode::ResourceExhausted,
        StorageError::KnowledgeGraphNotFound(_) | StorageError::RelationNotFound(..) => {
            ErrorCode::NotFound
        }
        StorageError::KnowledgeGraphExists(_)
        | StorageError::CannotDropDefault
        | StorageError::CannotDropCurrentKnowledgeGraph
        | StorageError::RelationNegated { .. }
        | StorageError::RuleNegated { .. } => ErrorCode::Conflict,
        StorageError::InvalidName(_)
        | StorageError::ParseError(_)
        | StorageError::WriteRejected(_) => ErrorCode::Validation,
        _ => default,
    }
}

/// `code` for a failed index command: `missing` when the index does not
/// exist (afterwards), `present` when it does.
fn index_error_code(
    storage: &StorageEngine,
    kg: &str,
    name: &str,
    missing: ErrorCode,
    present: ErrorCode,
) -> ErrorCode {
    match storage.index_stats_in(kg, None) {
        Ok(stats) if stats.iter().any(|s| s.name == name) => present,
        Ok(_) => missing,
        Err(e) => storage_error_code(&e, ErrorCode::Internal),
    }
}

fn internal_kg_denied() -> String {
    format!(
        "Access denied: '{}' is a system knowledge graph",
        crate::auth::INTERNAL_KG
    )
}

/// Format a rule as IQL text (uses Rule's Display impl)
fn format_rule_text(rule: &crate::ast::Rule) -> String {
    rule.to_string()
}

/// Format a body predicate as IQL text (uses BodyPredicate's Display impl)
fn format_body_pred(pred: &crate::ast::BodyPredicate) -> String {
    pred.to_string()
}

/// Format a term as IQL text (uses Term's Display impl)
fn format_term(term: &Term) -> String {
    term.to_string()
}

/// Statements in `program`, counted the way `parse_program` splits them.
/// A program's final result: an error if `auth` was revoked meanwhile, or
/// when the program was one failed statement.
fn settle_result(
    result: QueryResult,
    auth: Option<&crate::auth::Principal>,
    single_statement: bool,
) -> Result<QueryResult, ProgramError> {
    if let Some(principal) = auth {
        principal
            .identity()
            .map_err(|revoked| ProgramError::from(String::from(revoked)))?;
    }
    match result.errors.as_slice() {
        [error] if single_statement && result.switched_kg.is_none() => Err(ProgramError {
            message: error.message.clone(),
            code: Some(error.code),
        }),
        _ => Ok(result),
    }
}

fn program_statement_count(program: &str) -> usize {
    join_continuation_lines(&strip_comments(program))
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count()
}

/// Extract meaningful column names from a query's head variables.
///
/// Parses the query program and inspects the last rule's head atom arguments
/// to derive column names. Falls back to `col0, col1, ...` if parsing fails
/// or arity doesn't match.
pub(crate) fn extract_column_names_from_query(program: &str, arity: usize) -> Vec<String> {
    let parsed = match crate::parser::parse_program(program) {
        Ok(p) => p,
        Err(_) => return (0..arity).map(|i| format!("col{i}")).collect(),
    };

    let rule = match parsed.rules.last() {
        Some(r) => r,
        None => return (0..arity).map(|i| format!("col{i}")).collect(),
    };

    let head_args = &rule.head.args;
    if head_args.len() != arity {
        return (0..arity).map(|i| format!("col{i}")).collect();
    }

    // Build a map from parser-generated constant variables (_c0, _c1, ...)
    // to their bound constant values. The parser desugars constants in query
    // heads (e.g., ?tc(1, X)) into variables with equality constraints
    // (e.g., __query__(_c0, X) <- tc(_c0, X), _c0 = 1).
    let mut const_bindings: std::collections::HashMap<&str, String> =
        std::collections::HashMap::new();
    for pred in &rule.body {
        if let crate::ast::BodyPredicate::Comparison(lhs, crate::ast::ComparisonOp::Equal, rhs) =
            pred
        {
            let (var_name, bound_term) = match (lhs, rhs) {
                (Term::Variable(v), other) | (other, Term::Variable(v)) if v.starts_with("_c") => {
                    (v.as_str(), other)
                }
                _ => continue,
            };
            let val = match bound_term {
                Term::Constant(n) => format!("{n}"),
                Term::FloatConstant(f) => format!("{f}"),
                Term::StringConstant(s) => s.clone(),
                Term::BoolConstant(b) => format!("{b}"),
                _ => continue,
            };
            const_bindings.insert(var_name, val);
        }
    }

    head_args
        .iter()
        .enumerate()
        .map(|(i, term)| match term {
            Term::Variable(v) => {
                if let Some(val) = const_bindings.get(v.as_str()) {
                    val.clone()
                } else if v.starts_with("_p") {
                    "_".to_string()
                } else {
                    v.clone()
                }
            }
            Term::Aggregate(func, var) => format!("{}_{}", format!("{func:?}").to_lowercase(), var),
            Term::Constant(n) => format!("{n}"),
            Term::FloatConstant(f) => format!("{f}"),
            Term::StringConstant(s) => s.clone(),
            Term::BoolConstant(b) => format!("{b}"),
            Term::Placeholder => "_".to_string(),
            _ => format!("col{i}"),
        })
        .collect()
}

/// Find the source relation for a query.
///
/// For `__query__` shorthand queries, returns the first positive body atom's relation.
/// For named head relations, returns the head relation name.
/// Used to look up registered schemas in the SchemaCatalog.
fn find_query_source_relation(program: &str) -> Option<String> {
    let parsed = crate::parser::parse_program(program).ok()?;
    let rule = parsed.rules.last()?;

    if rule.head.relation == "__query__" {
        // Shorthand query: find the first positive body atom
        for pred in &rule.body {
            if let crate::ast::BodyPredicate::Positive(atom) = pred {
                return Some(atom.relation.clone());
            }
        }
        None
    } else {
        Some(rule.head.relation.clone())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::ast::Term;
    use std::time::Duration;

    /// Create a Config with a unique temp directory. Returns TempDir so it stays alive
    /// for the test's duration and auto-cleans on drop.
    fn make_test_config() -> (Config, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("failed to create temp dir");
        let mut config = Config::default();
        config.storage.data_dir = tmp.path().to_path_buf();
        (config, tmp)
    }

    /// Convenience: create a Handler with isolated temp storage.
    fn make_test_handler() -> (Handler, tempfile::TempDir) {
        let (config, tmp) = make_test_config();
        (
            Handler::from_config(config).expect("handler creation failed"),
            tmp,
        )
    }

    /// Convenience: create a StorageEngine with isolated temp storage.
    fn make_test_storage() -> (StorageEngine, tempfile::TempDir) {
        let (config, tmp) = make_test_config();
        (
            StorageEngine::new(config).expect("storage creation failed"),
            tmp,
        )
    }

    #[tokio::test]
    async fn durability_graph_switch_survives_unknown_owner_grant() {
        use crate::storage::persist::wal::WalFault;
        let (mut config, _temp) = make_test_config();
        config.storage.persist.durability_mode = crate::config::DurabilityMode::Immediate;
        config.http.auth.bootstrap_admin_password = Some("password123".to_string());
        let handler = Handler::from_config(config).unwrap();
        handler.bootstrap_auth();
        handler
            .handle_user_create("editor", "password123", "editor")
            .unwrap();
        handler
            .handle_kg_acl_grant("default", "editor", "editor")
            .unwrap();
        let principal = handler.authenticate_user("editor", "password123").unwrap();
        let session = handler
            .create_session_with_auth("default", &principal)
            .unwrap();
        for fault in [WalFault::Sync, WalFault::Restore, WalFault::SaveCut] {
            handler.storage.read().inject_wal_fault(fault);
        }
        let result = handler
            .execute_program_status(
                Some(&session),
                None,
                ".kg create new_graph".to_string(),
                Some(&principal),
                &handler.request_control(None),
            )
            .await
            .unwrap();
        assert_eq!(result.switched_kg.as_deref(), Some("new_graph"));
        assert_eq!(handler.sessions.session_kg(&session).unwrap(), "new_graph");
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].code, ErrorCode::OutcomeUnknown);
        let switched_back = handler
            .execute_program_status(
                Some(&session),
                None,
                ".kg use default".to_string(),
                Some(&principal),
                &handler.request_control(None),
            )
            .await
            .unwrap();
        assert_eq!(switched_back.switched_kg.as_deref(), Some("default"));
        assert!(switched_back.errors.is_empty());
    }

    #[tokio::test]
    async fn durability_protocol_preserves_unknown_and_read_only() {
        use crate::storage::persist::wal::WalFault;
        for program in ["+r(1)", "+r(1)\n+r(2)"] {
            let (mut config, _temp) = make_test_config();
            config.storage.persist.durability_mode = crate::config::DurabilityMode::Immediate;
            let handler = Handler::from_config(config).unwrap();
            for fault in [WalFault::Sync, WalFault::Restore, WalFault::SaveCut] {
                handler.storage.read().inject_wal_fault(fault);
            }
            let result = handler
                .execute_program_status(
                    None,
                    None,
                    program.to_string(),
                    None,
                    &handler.request_control(None),
                )
                .await;
            if program.contains('\n') {
                let result = result.unwrap();
                assert_eq!(result.errors[0].code, ErrorCode::OutcomeUnknown);
                assert!(!result.errors[0].message.contains("nothing was applied"));
                assert!(!result.errors[0].message.contains("rolled back"));
            } else {
                assert_eq!(result.unwrap_err().code, Some(ErrorCode::OutcomeUnknown));
            }
            let error = handler
                .execute_program_status(
                    None,
                    None,
                    "+r(3)".to_string(),
                    None,
                    &handler.request_control(None),
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, Some(ErrorCode::StoreReadOnly));
            let result = handler
                .execute_program_status(
                    None,
                    None,
                    "?r(X)".to_string(),
                    None,
                    &handler.request_control(None),
                )
                .await;
            assert!(!matches!(
                result,
                Err(ProgramError {
                    code: Some(ErrorCode::StoreReadOnly),
                    ..
                })
            ));
        }
    }

    #[tokio::test]
    async fn durability_ontology_preserves_unknown_outcomes() {
        use crate::storage::persist::wal::WalFault;
        for command in [".ontology remove demo", ".ontology upgrade demo"] {
            let (mut config, _temp) = make_test_config();
            config.storage.persist.durability_mode = crate::config::DurabilityMode::Immediate;
            let handler = Handler::from_config(config).unwrap();
            handler.execute_program_status(None, None,
                "+seed(1)\n+r(X) <- seed(X)\n+pack_meta[(\"demo\", \"1\", \"sha\")]\n+pack_item[(\"demo\", \"rule\", \"r\")]".to_string(), None, &handler.request_control(None)).await.unwrap();
            for fault in [WalFault::Sync, WalFault::Restore, WalFault::SaveCut] {
                handler.storage.read().inject_wal_fault(fault);
            }
            let error = handler
                .execute_program_status(
                    None,
                    None,
                    command.to_string(),
                    None,
                    &handler.request_control(None),
                )
                .await
                .unwrap_err();
            assert_eq!(
                error.code,
                Some(ErrorCode::OutcomeUnknown),
                "{command}: {error}"
            );
            assert!(!error.message.contains("nothing was applied"));
            let error = handler
                .execute_program_status(
                    None,
                    None,
                    ".ontology remove demo".to_string(),
                    None,
                    &handler.request_control(None),
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, Some(ErrorCode::StoreReadOnly));
        }
    }

    #[test]
    fn user_drop_cleanup_continues_after_transient_failure() {
        use crate::auth::{Role, INTERNAL_KG};
        use crate::storage::persist::wal::WalFault;

        for with_key in [true, false] {
            let (mut config, _temp) = make_test_config();
            config.storage.persist.durability_mode = crate::config::DurabilityMode::Immediate;
            config.http.auth.bootstrap_admin_password = Some("password123".to_string());
            let handler = Handler::from_config(config.clone()).unwrap();
            handler.bootstrap_auth();
            handler
                .handle_user_create("bob", "password123", "viewer")
                .unwrap();
            handler
                .storage
                .write()
                .create_knowledge_graph("private")
                .unwrap();
            handler
                .handle_kg_acl_grant("private", "bob", "viewer")
                .unwrap();
            let key = with_key.then(|| handler.create_api_key("old-key", "bob", None).unwrap());
            handler.storage.read().inject_wal_fault(WalFault::Sync);

            let error = handler.handle_user_drop("bob").unwrap_err();
            assert_ne!(error.code, Some(ErrorCode::OutcomeUnknown));
            assert_ne!(error.code, Some(ErrorCode::StoreReadOnly));
            let snapshot = handler
                .storage
                .read()
                .get_snapshot_for(INTERNAL_KG)
                .unwrap();
            assert!(snapshot.input_tuples["users"]
                .iter()
                .any(|tuple| { tuple.values()[0].as_str() == Some("bob") }));
            assert_eq!(
                handler
                    .get_kg_role_for_user("private", "bob", &Role::Viewer)
                    .is_none(),
                with_key
            );
            assert!(handler
                .handle_user_create("bob", "password456", "viewer")
                .is_err());

            handler.handle_user_drop("bob").unwrap();
            handler
                .handle_user_create("bob", "password456", "viewer")
                .unwrap();
            assert!(handler
                .get_kg_role_for_user("private", "bob", &Role::Viewer)
                .is_none());
            if let Some(key) = &key {
                assert!(handler.authenticate_api_key(key).is_err());
            }
            handler.shutdown();
            drop(handler);
            let reopened = Handler::from_config(config).unwrap();
            reopened.bootstrap_auth();
            assert!(reopened.authenticate_user("bob", "password456").is_ok());
            assert!(reopened
                .get_kg_role_for_user("private", "bob", &Role::Viewer)
                .is_none());
            if let Some(key) = &key {
                assert!(reopened.authenticate_api_key(key).is_err());
            }
        }
    }

    #[tokio::test]
    async fn kg_drop_keeps_applied_result_when_acl_cleanup_fails() {
        use crate::storage::persist::wal::WalFault;

        for (faults, typed) in [
            (vec![WalFault::Sync], None),
            (
                vec![WalFault::Sync, WalFault::Restore, WalFault::SaveCut],
                Some(ErrorCode::OutcomeUnknown),
            ),
        ] {
            let (mut config, _temp) = make_test_config();
            config.storage.persist.durability_mode = crate::config::DurabilityMode::Immediate;
            config.http.auth.bootstrap_admin_password = Some("password123".to_string());
            let handler = Handler::from_config(config).unwrap();
            handler.bootstrap_auth();
            handler.storage.write().create_knowledge_graph("g").unwrap();
            handler
                .handle_user_create("bob", "password123", "viewer")
                .unwrap();
            handler.handle_kg_acl_grant("g", "bob", "viewer").unwrap();
            for fault in faults {
                handler.storage.read().inject_wal_fault(fault);
            }

            let outcome = handler
                .execute_program_status(
                    None,
                    None,
                    ".kg drop g".to_string(),
                    None,
                    &handler.request_control(None),
                )
                .await;
            assert!(handler.storage.read().get_snapshot_for("g").is_err());
            match typed {
                Some(code) => assert_eq!(outcome.unwrap_err().code, Some(code)),
                None => {
                    let result = outcome.unwrap();
                    assert!(result.errors.is_empty());
                    let messages: Vec<_> = result
                        .rows
                        .iter()
                        .map(|row| format!("{:?}", row.values[0]))
                        .collect();
                    assert!(messages[0].contains("Knowledge graph 'g' dropped."));
                    assert!(messages[1].contains("were not removed"), "{messages:?}");
                    assert_eq!(result.total_count, 2);
                }
            }
        }
    }

    #[test]
    fn user_drop_cleanup_preserves_unknown_outcome() {
        use crate::storage::persist::wal::WalFault;

        let (mut config, _temp) = make_test_config();
        config.storage.persist.durability_mode = crate::config::DurabilityMode::Immediate;
        config.http.auth.bootstrap_admin_password = Some("password123".to_string());
        let handler = Handler::from_config(config).unwrap();
        handler.bootstrap_auth();
        handler
            .handle_user_create("bob", "password123", "viewer")
            .unwrap();
        handler.create_api_key("old-key", "bob", None).unwrap();
        handler
            .handle_kg_acl_grant("default", "bob", "viewer")
            .unwrap();
        for fault in [WalFault::Sync, WalFault::Restore, WalFault::SaveCut] {
            handler.storage.read().inject_wal_fault(fault);
        }
        assert_eq!(
            handler.handle_user_drop("bob").unwrap_err().code,
            Some(ErrorCode::OutcomeUnknown)
        );
        assert_eq!(
            handler.handle_user_drop("bob").unwrap_err().code,
            Some(ErrorCode::StoreReadOnly)
        );
    }

    #[tokio::test]
    async fn durability_admin_protocol_preserves_outcome_types() {
        use crate::storage::persist::wal::WalFault;
        for command in [
            ".user create alice password123 viewer",
            ".user drop bob",
            ".user password bob password456",
            ".user role bob editor",
            ".apikey create new-key",
            ".apikey revoke old-key",
            ".kg acl grant default bob editor",
            ".kg acl revoke default bob",
        ] {
            let (mut config, _temp) = make_test_config();
            config.storage.persist.durability_mode = crate::config::DurabilityMode::Immediate;
            config.http.auth.bootstrap_admin_password = Some("password123".to_string());
            let handler = Handler::from_config(config).unwrap();
            handler.bootstrap_auth();
            handler
                .handle_user_create("bob", "password123", "viewer")
                .unwrap();
            handler.create_api_key("old-key", "bob", None).unwrap();
            handler
                .handle_kg_acl_grant("default", "bob", "viewer")
                .unwrap();
            for fault in [WalFault::Sync, WalFault::Restore, WalFault::SaveCut] {
                handler.storage.read().inject_wal_fault(fault);
            }
            let error = handler
                .execute_program_status(
                    None,
                    None,
                    command.to_string(),
                    None,
                    &handler.request_control(None),
                )
                .await
                .unwrap_err();
            assert_eq!(
                error.code,
                Some(ErrorCode::OutcomeUnknown),
                "{command}: {error}"
            );
            let error = handler
                .execute_program_status(
                    None,
                    None,
                    command.to_string(),
                    None,
                    &handler.request_control(None),
                )
                .await
                .unwrap_err();
            assert_eq!(
                error.code,
                Some(ErrorCode::StoreReadOnly),
                "{command}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn test_login_outcome_recorded_after_caller_gives_up() {
        let (mut config, _tmp) = make_test_config();
        config.http.auth.bootstrap_admin_password = Some("pw".to_string());
        let handler = Arc::new(Handler::from_config(config).expect("handler creation failed"));
        handler.bootstrap_auth();
        let peer = std::net::IpAddr::from([192, 0, 2, 1]);
        for _ in 0..4 {
            let attempt = handler.login_throttle.begin(peer, "admin").unwrap();
            handler.login_throttle.fail(&attempt);
        }
        let login = handler.login("admin", "pw", peer);
        assert!(tokio::time::timeout(Duration::ZERO, login).await.is_err());
        // Wait for the abandoned login to settle.
        let _all = handler
            .login_queue
            .acquire_many(MAX_QUEUED_LOGINS as u32)
            .await
            .unwrap();
        assert!(
            handler.login_throttle.begin(peer, "admin").is_ok(),
            "the abandoned correct login counts as a success"
        );
    }

    // --- term_to_value tests ---

    #[test]
    fn test_term_to_value_int() {
        assert_eq!(
            term_to_value(&Term::Constant(42)).expect("term conversion failed"),
            Value::Int64(42)
        );
    }

    #[test]
    fn test_term_to_value_float() {
        assert_eq!(
            term_to_value(&Term::FloatConstant(3.14)).expect("term conversion failed"),
            Value::Float64(3.14)
        );
    }

    #[test]
    fn test_term_to_value_string() {
        assert_eq!(
            term_to_value(&Term::StringConstant("hello".to_string()))
                .expect("term conversion failed"),
            Value::string("hello")
        );
    }

    #[test]
    fn test_term_to_value_bool() {
        assert_eq!(
            term_to_value(&Term::BoolConstant(true)).expect("term conversion failed"),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_term_to_value_vector() {
        let result = term_to_value(&Term::VectorLiteral(vec![1.0, 2.0, 3.0]))
            .expect("term conversion failed");
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
            term_to_value(&Term::Constant(-100)).expect("term conversion failed"),
            Value::Int64(-100)
        );
    }

    #[test]
    fn test_term_to_value_zero() {
        assert_eq!(
            term_to_value(&Term::Constant(0)).expect("term conversion failed"),
            Value::Int64(0)
        );
    }

    // --- Handler construction tests ---

    #[test]
    fn test_handler_new() {
        let (storage, _tmp) = make_test_storage();
        let handler = Handler::new(storage);
        assert_eq!(handler.total_queries(), 0);
        assert_eq!(handler.total_inserts(), 0);
        assert!(handler.uptime_seconds() < 2);
    }

    #[test]
    fn test_handler_from_config() {
        let (handler, _tmp) = make_test_handler();
        assert_eq!(handler.total_queries(), 0);
    }

    #[test]
    fn test_handler_with_session_config() {
        let (storage, _tmp) = make_test_storage();
        let config = SessionConfig {
            max_sessions: 10,
            ..SessionConfig::default()
        };
        let handler = Handler::with_session_config(storage, config);
        assert_eq!(handler.total_queries(), 0);
    }

    // --- Session management tests ---

    /// Helper to create a fresh handler with a known KG and isolated temp storage.
    fn handler_with_kg(kg_name: &str) -> (Handler, tempfile::TempDir) {
        let (mut config, tmp) = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        let handler = Handler::from_config(config).expect("handler creation failed");
        handler
            .get_storage()
            .ensure_knowledge_graph(kg_name)
            .expect("knowledge graph creation failed");
        (handler, tmp)
    }

    #[test]
    fn test_handler_create_and_close_session() {
        let (handler, _tmp) = handler_with_kg("sess_create_test");
        let session_id = handler
            .create_session("sess_create_test")
            .expect("session creation failed");
        assert!(!session_id.is_empty());
        handler
            .close_session(&session_id)
            .expect("session close failed");
    }

    #[test]
    fn test_handler_close_nonexistent_session() {
        let (handler, _tmp) = make_test_handler();
        assert!(handler.close_session(&"nonexistent".to_string()).is_err());
    }

    #[test]
    fn test_handler_session_insert_ephemeral() {
        let (handler, _tmp) = handler_with_kg("sess_insert_test");
        let sid = handler
            .create_session("sess_insert_test")
            .expect("session creation failed");
        let tuples = vec![Tuple::new(vec![Value::Int64(1), Value::Int64(2)])];
        handler
            .session_insert_ephemeral(&sid, "edge", tuples)
            .expect("ephemeral insert failed");
    }

    #[test]
    fn test_handler_session_retract_ephemeral() {
        let (handler, _tmp) = handler_with_kg("sess_retract_test");
        let sid = handler
            .create_session("sess_retract_test")
            .expect("session creation failed");
        let tuples = vec![Tuple::new(vec![Value::Int64(1)])];
        handler
            .session_insert_ephemeral(&sid, "r", tuples.clone())
            .expect("ephemeral insert failed");
        let retracted = handler
            .session_retract_ephemeral(&sid, "r", tuples)
            .expect("ephemeral retract failed");
        assert_eq!(retracted, 1);
    }

    #[test]
    fn test_handler_session_stats() {
        let (handler, _tmp) = make_test_handler();
        let stats = handler.session_stats();
        assert_eq!(stats.total_sessions, 0);
    }

    // --- Notification tests ---

    #[test]
    fn test_subscribe_notifications() {
        let (storage, _tmp) = make_test_storage();
        let handler = Handler::new(storage);
        let mut rx = handler.subscribe_notifications();

        handler.notify_persistent_update("test_kg", "edge", "insert", 5);

        match rx.try_recv() {
            Ok(Notification::PersistentUpdate {
                knowledge_graph,
                relation,
                operation,
                count,
                ..
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
        let (storage, _tmp) = make_test_storage();
        let handler = Handler::new(storage);
        handler.notify_persistent_update("test_kg", "edge", "insert", 1);
    }

    #[test]
    fn test_multiple_subscribers() {
        let (storage, _tmp) = make_test_storage();
        let handler = Handler::new(storage);
        let mut rx1 = handler.subscribe_notifications();
        let mut rx2 = handler.subscribe_notifications();

        handler.notify_persistent_update("kg", "rel", "delete", 3);

        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    #[test]
    fn test_notification_seq_increments() {
        let (storage, _tmp) = make_test_storage();
        let handler = Handler::new(storage);
        let mut rx = handler.subscribe_notifications();

        handler.notify_persistent_update("kg", "a", "insert", 1);
        handler.notify_persistent_update("kg", "b", "insert", 2);
        handler.notify_kg_change("kg2", "created");

        let n1 = rx.try_recv().expect("notification receive failed");
        let n2 = rx.try_recv().expect("notification receive failed");
        let n3 = rx.try_recv().expect("notification receive failed");
        assert_eq!(n1.seq(), 1);
        assert_eq!(n2.seq(), 2);
        assert_eq!(n3.seq(), 3);
    }

    #[test]
    fn test_notification_ring_retains_the_configured_buffer_size() {
        use crate::protocol::notification_log::{Cursor, ReplayGap};
        let (storage, _tmp) = make_test_storage();
        let handler = Handler::new(storage);
        let buf_size = handler.config().http.rate_limit.notification_buffer_size;

        for i in 0..(buf_size + 10) {
            handler.notify_persistent_update("kg", &format!("r{i}"), "insert", 1);
        }

        let log = handler.notifications();
        let cursor = |last_seq| Cursor {
            epoch: Some(log.epoch().to_string()),
            last_seq,
        };
        // The oldest 10 were evicted: history after seq 10 is complete.
        let retained = log.resume(Some(&cursor(10))).replay.unwrap();
        assert_eq!(retained.len(), buf_size);
        assert_eq!(retained[0].seq(), 11);
        assert_eq!(
            log.resume(Some(&cursor(9))).replay.unwrap_err(),
            ReplayGap::Evicted {
                oldest_retained: 11
            }
        );
    }

    // --- query_program tests ---

    #[tokio::test]
    async fn test_query_program_simple_insert() {
        // Use unique KG name to avoid leftover data from previous test runs
        let (handler, _tmp) = handler_with_kg("simple_insert_test");
        let result = handler
            .query_program(
                Some("simple_insert_test".to_string()),
                "+edge[(1,2), (3,4)]".to_string(),
            )
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 1);
        let actual = result.rows[0].values[0]
            .as_str()
            .expect("expected string value");
        assert!(
            actual.contains("Inserted 2"),
            "Expected 'Inserted 2', got: {actual}"
        );
    }

    #[tokio::test]
    async fn test_query_program_insert_and_query() {
        let (handler, _tmp) = make_test_handler();
        handler
            .query_program(None, "+data[(1,), (2,), (3,)]".to_string())
            .await
            .expect("query execution failed");
        let result = handler
            .query_program(None, "?data(X)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 3);
    }

    #[tokio::test]
    async fn test_query_program_comment_stripping() {
        let (handler, _tmp) = make_test_handler();
        let program = "% this is a comment\n// this too\n+test_data[(1,)]".to_string();
        let result = handler
            .query_program(None, program)
            .await
            .expect("query execution failed");
        assert!(result.rows[0].values[0]
            .as_str()
            .expect("operation should succeed")
            .contains("Inserted"));
    }

    #[tokio::test]
    async fn test_query_program_session_fact() {
        let (handler, _tmp) = make_test_handler();
        let program = "temp(42)\n?temp(X)".to_string();
        let result = handler
            .query_program(None, program)
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_query_program_session_rule() {
        let (handler, _tmp) = make_test_handler();
        handler
            .query_program(None, "+base[(1,), (2,), (3,)]".to_string())
            .await
            .expect("query execution failed");
        let program = "doubled(X, Y) <- base(X), Y = X * 2\n?doubled(X, Y)".to_string();
        let result = handler
            .query_program(None, program)
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 3);
    }

    #[tokio::test]
    async fn test_query_program_persistent_rule() {
        let (handler, _tmp) = make_test_handler();
        handler
            .query_program(None, "+nodes[(1,), (2,)]".to_string())
            .await
            .expect("query execution failed");
        handler
            .query_program(None, "+big(X) <- nodes(X), X > 1".to_string())
            .await
            .expect("query execution failed");
        let result = handler
            .query_program(None, "?big(X)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_query_program_delete_facts() {
        let (handler, _tmp) = make_test_handler();
        handler
            .query_program(None, "+del_test[(1, 2), (3, 4)]".to_string())
            .await
            .expect("query execution failed");
        let result = handler
            .query_program(None, "-del_test(1, 2)".to_string())
            .await
            .expect("query execution failed");
        assert!(result.rows[0].values[0]
            .as_str()
            .expect("operation should succeed")
            .contains("Deleted"));
        let remaining = handler
            .query_program(None, "?del_test(X, Y)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(remaining.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_query_program_with_target_kg() {
        let (handler, _tmp) = handler_with_kg("handler_target_kg");
        let result = handler
            .query_program(
                Some("handler_target_kg".to_string()),
                "+kgdata[(1,)]".to_string(),
            )
            .await
            .expect("query execution failed");
        assert!(result.rows[0].values[0]
            .as_str()
            .expect("operation should succeed")
            .contains("Inserted"));
    }

    #[tokio::test]
    async fn test_query_program_no_results() {
        let (handler, _tmp) = make_test_handler();
        let result = handler
            .query_program(None, "?empty_relation(X)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 0);
    }

    // --- debug tests ---

    #[test]
    fn test_debug_query_simple() {
        let (handler, _tmp) = handler_with_kg("debug_test_kg");
        // debug_query takes an IQL rule, not a ?query
        let result = handler.debug_query(
            Some("debug_test_kg".to_string()),
            "__q__(X, Y) <- edge(X, Y)".to_string(),
        );
        assert!(result.is_ok(), "debug failed: {:?}", result.err());
        let (trace, optimizations) = result.expect("operation should succeed");
        assert!(!trace.is_empty());
        assert!(!optimizations.is_empty());
    }

    #[test]
    fn test_debug_query_no_kg_error() {
        let (storage, _tmp) = make_test_storage();
        let handler = Handler::new(storage);
        let result = handler.debug_query(None, "?edge(X, Y)".to_string());
        // No current KG selected → error
        assert!(result.is_err());
    }

    #[test]
    fn test_debug_query_join() {
        let (handler, _tmp) = handler_with_kg("debug_join_kg");
        let result = handler.debug_query(
            Some("debug_join_kg".to_string()),
            "__q__(X, Z) <- edge(X, Y), edge(Y, Z)".to_string(),
        );
        assert!(result.is_ok(), "debug join failed: {:?}", result.err());
        let (trace, _) = result.expect("operation should succeed");
        assert!(!trace.is_empty());
    }

    #[test]
    fn test_debug_query_recursive() {
        let (handler, _tmp) = handler_with_kg("debug_rec_kg");
        let result = handler.debug_query(
            Some("debug_rec_kg".to_string()),
            "__q__(X, Y) <- edge(X, Y)".to_string(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_debug_query_nonexistent_kg() {
        let (storage, _tmp) = make_test_storage();
        let handler = Handler::new(storage);
        let result = handler.debug_query(
            Some("nonexistent_kg".to_string()),
            "__q__(X, Y) <- edge(X, Y)".to_string(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn test_debug_query_returns_optimization_list() {
        let (handler, _tmp) = handler_with_kg("debug_opt_kg");
        let (_, optimizations) = handler
            .debug_query(
                Some("debug_opt_kg".to_string()),
                "__q__(X, Y) <- edge(X, Y)".to_string(),
            )
            .expect("operation should succeed");
        assert!(optimizations.len() >= 4);
        assert!(optimizations.iter().any(|o| o.contains("Join Planning")));
        assert!(optimizations.iter().any(|o| o.contains("SIP")));
        assert!(optimizations.iter().any(|o| o.contains("Subplan Sharing")));
    }

    // --- query_program edge case tests ---

    #[tokio::test]
    async fn test_query_program_schema_declaration() {
        let (handler, _tmp) = handler_with_kg("schema_decl_test");
        // Persistent schema syntax: +name(col: type, ...)
        let result = handler
            .query_program(
                Some("schema_decl_test".to_string()),
                "+person(name: string, age: int)".to_string(),
            )
            .await
            .expect("query execution failed");
        assert!(result.rows[0].values[0]
            .as_str()
            .expect("operation should succeed")
            .contains("Schema"));
    }

    #[tokio::test]
    async fn test_query_program_bulk_delete() {
        let (handler, _tmp) = handler_with_kg("bulk_del_test");
        handler
            .query_program(
                Some("bulk_del_test".to_string()),
                "+bd_rel[(1, 2), (3, 4), (5, 6)]".to_string(),
            )
            .await
            .expect("query execution failed");
        let result = handler
            .query_program(
                Some("bulk_del_test".to_string()),
                "-bd_rel[(1, 2), (3, 4)]".to_string(),
            )
            .await
            .expect("query execution failed");
        assert!(result.rows[0].values[0]
            .as_str()
            .expect("operation should succeed")
            .contains("Deleted"));
        // Verify only 1 fact remains
        let remaining = handler
            .query_program(
                Some("bulk_del_test".to_string()),
                "?bd_rel(X, Y)".to_string(),
            )
            .await
            .expect("query execution failed");
        assert_eq!(remaining.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_query_program_register_persistent_rule() {
        let (handler, _tmp) = handler_with_kg("persist_rule_test");
        handler
            .query_program(
                Some("persist_rule_test".to_string()),
                "+vals[(1,), (2,), (3,)]".to_string(),
            )
            .await
            .expect("query execution failed");
        let result = handler
            .query_program(
                Some("persist_rule_test".to_string()),
                "+doubled(X, Y) <- vals(X), Y = X * 2".to_string(),
            )
            .await
            .expect("query execution failed");
        assert!(result.rows[0].values[0]
            .as_str()
            .expect("expected string value")
            .contains("Rule"));
    }

    #[tokio::test]
    async fn test_query_program_conditional_delete() {
        let (handler, _tmp) = handler_with_kg("cond_del_test");
        handler
            .query_program(
                Some("cond_del_test".to_string()),
                "+items[(1, 10), (2, 20), (3, 30)]".to_string(),
            )
            .await
            .expect("query execution failed");
        let result = handler
            .query_program(
                Some("cond_del_test".to_string()),
                "-items(X, Y) <- Y > 15".to_string(),
            )
            .await
            .expect("query execution failed");
        assert!(result.rows[0].values[0]
            .as_str()
            .expect("operation should succeed")
            .contains("delete"));
        // Verify only (1, 10) remains
        let remaining = handler
            .query_program(
                Some("cond_del_test".to_string()),
                "?items(X, Y)".to_string(),
            )
            .await
            .expect("query execution failed");
        assert_eq!(remaining.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_query_program_empty_program() {
        let (handler, _tmp) = handler_with_kg("empty_prog_test");
        // Empty program has no IR nodes to execute
        let result = handler
            .query_program(Some("empty_prog_test".to_string()), String::new())
            .await;
        assert!(result.is_err() || result.expect("operation should succeed").rows.len() <= 1);
    }

    #[tokio::test]
    async fn test_query_program_only_comments() {
        let (handler, _tmp) = handler_with_kg("comments_only_test");
        // Program with only comments has no IR nodes to execute
        let result = handler
            .query_program(
                Some("comments_only_test".to_string()),
                "% just a comment\n// another comment".to_string(),
            )
            .await;
        assert!(result.is_err() || result.expect("operation should succeed").rows.len() <= 1);
    }

    #[tokio::test]
    async fn test_query_program_multiple_queries_last_wins() {
        let (handler, _tmp) = handler_with_kg("multi_q_test");
        handler
            .query_program(
                Some("multi_q_test".to_string()),
                "+alpha[(1,)]\n+beta[(2,)]".to_string(),
            )
            .await
            .expect("query execution failed");
        // When multiple queries exist, only the last query is executed
        let result = handler
            .query_program(
                Some("multi_q_test".to_string()),
                "?alpha(X)\n?beta(X)".to_string(),
            )
            .await
            .expect("query execution failed");
        // Last query is ?beta(X), should return 1 row
        assert_eq!(result.rows.len(), 1);
    }

    // --- query_program_with_session tests ---

    #[tokio::test]
    async fn test_query_program_with_session_clean() {
        let (handler, _tmp) = handler_with_kg("sess_clean_q");
        handler
            .query_program(
                Some("sess_clean_q".to_string()),
                "+sdata[(1,), (2,)]".to_string(),
            )
            .await
            .expect("query execution failed");
        let sid = handler
            .create_session("sess_clean_q")
            .expect("session creation failed");
        // Clean session uses fast path (delegates to query_program)
        let result = handler
            .query_program_with_session(&sid, "?sdata(X)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 2);
        handler.close_session(&sid).expect("session close failed");
    }

    #[tokio::test]
    async fn test_query_program_with_session_ephemeral_facts() {
        let (handler, _tmp) = handler_with_kg("sess_eph_q");
        handler
            .query_program(Some("sess_eph_q".to_string()), "+data[(1,)]".to_string())
            .await
            .expect("query execution failed");
        let sid = handler
            .create_session("sess_eph_q")
            .expect("session creation failed");
        handler
            .session_insert_ephemeral(&sid, "data", vec![Tuple::new(vec![Value::Int64(2)])])
            .expect("ephemeral insert failed");
        // query_program_with_session takes raw IQL rules, not ?shorthand
        let result = handler
            .query_program_with_session(&sid, "__q__(X) <- data(X)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 2);
        handler.close_session(&sid).expect("session close failed");
    }

    #[tokio::test]
    async fn test_query_program_with_session_invalid_id_falls_back() {
        let (handler, _tmp) = make_test_handler();
        // Invalid session should gracefully fall back to non-session query
        // (returns Ok with empty results, not an error)
        let result = handler
            .query_program_with_session(&"99999".to_string(), "?data(X)".to_string())
            .await;
        assert!(
            result.is_ok(),
            "invalid session should fall back to query_program, not error"
        );
    }

    // --- session_add_rule test ---

    #[tokio::test]
    async fn test_session_add_rule_and_query() {
        let (handler, _tmp) = handler_with_kg("sess_rule_q");
        handler
            .query_program(
                Some("sess_rule_q".to_string()),
                "+base[(1,), (2,), (3,)]".to_string(),
            )
            .await
            .expect("query execution failed");
        let sid = handler
            .create_session("sess_rule_q")
            .expect("session creation failed");
        // Parse a rule and add it to the session
        let rule_text = "doubled(X, Y) <- base(X), Y = X * 2";
        let rule = crate::parser::parse_rule(rule_text).expect("rule parsing failed");
        handler
            .session_add_rule(&sid, rule, rule_text.to_string())
            .expect("session add rule failed");
        // query_program_with_session takes raw IQL rules, not ?shorthand
        let result = handler
            .query_program_with_session(&sid, "__q__(X, Y) <- doubled(X, Y)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 3);
        handler.close_session(&sid).expect("session close failed");
    }

    // --- validate_tuples_against_schema tests ---

    #[tokio::test]
    async fn test_validate_tuples_no_schema() {
        let (handler, _tmp) = handler_with_kg("val_no_schema");
        let tuples = vec![Tuple::new(vec![Value::Int64(1)])];
        // No schema registered → validation passes
        assert!(handler
            .validate_tuples_against_schema("val_no_schema", "any_rel", &tuples)
            .is_ok());
    }

    #[tokio::test]
    async fn test_validate_tuples_with_schema() {
        let (handler, _tmp) = handler_with_kg("val_with_schema");
        // Persistent schema syntax: +name(col: type, ...)
        handler
            .query_program(
                Some("val_with_schema".to_string()),
                "+typed_rel(name: string, value: int)".to_string(),
            )
            .await
            .expect("query execution failed");
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
        let (handler, _tmp) = handler_with_kg("counter_test");
        assert_eq!(handler.total_queries(), 0);
        handler
            .query_program(Some("counter_test".to_string()), "+stuff[(1,)]".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(handler.total_queries(), 1);
        handler
            .query_program(Some("counter_test".to_string()), "?stuff(X)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(handler.total_queries(), 2);
    }

    #[tokio::test]
    async fn test_insert_count_increments() {
        let (handler, _tmp) = handler_with_kg("insert_cnt_test");
        assert_eq!(handler.total_inserts(), 0);
        handler
            .query_program(
                Some("insert_cnt_test".to_string()),
                "+data[(1,), (2,)]".to_string(),
            )
            .await
            .expect("query execution failed");
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
        let (handler, _tmp) = handler_with_kg("multi_client_1");
        let kg = "multi_client_1";

        // 1. Insert persistent base facts
        handler
            .query_program(Some(kg.to_string()), "+edge[(10, 20)]".to_string())
            .await
            .expect("query execution failed");

        // 2. Register persistent rule: reachable(X,Y) <- edge(X,Y)
        handler
            .query_program(
                Some(kg.to_string()),
                "+reachable(X, Y) <- edge(X, Y)".to_string(),
            )
            .await
            .expect("query execution failed");

        // 3. Create two sessions (two "clients")
        let client_a = handler.create_session(kg).expect("session creation failed");
        let client_b = handler.create_session(kg).expect("session creation failed");

        // 4. Client A inserts ephemeral edge(1, 2)
        handler
            .session_insert_ephemeral(
                &client_a,
                "edge",
                vec![Tuple::new(vec![Value::Int64(1), Value::Int64(2)])],
            )
            .expect("ephemeral insert failed");

        // 5. Client B inserts ephemeral edge(3, 4)
        handler
            .session_insert_ephemeral(
                &client_b,
                "edge",
                vec![Tuple::new(vec![Value::Int64(3), Value::Int64(4)])],
            )
            .expect("ephemeral insert failed");

        // 6. Client A queries reachable → sees persistent (10,20) + ephemeral (1,2)
        let result_a = handler
            .query_program_with_session(&client_a, "__q__(X, Y) <- reachable(X, Y)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(
            result_a.rows.len(),
            2,
            "Client A should see 2 reachable tuples: persistent (10,20) + ephemeral (1,2)"
        );

        // 7. Client B queries reachable → sees persistent (10,20) + ephemeral (3,4)
        let result_b = handler
            .query_program_with_session(&client_b, "__q__(X, Y) <- reachable(X, Y)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(
            result_b.rows.len(),
            2,
            "Client B should see 2 reachable tuples: persistent (10,20) + ephemeral (3,4)"
        );

        // 8. Verify the actual values differ between sessions
        let a_values: std::collections::HashSet<(i64, i64)> = result_a
            .rows
            .iter()
            .map(|r| {
                (
                    r.values[0].as_i64().expect("expected i64 value"),
                    r.values[1].as_i64().expect("expected i64 value"),
                )
            })
            .collect();
        let b_values: std::collections::HashSet<(i64, i64)> = result_b
            .rows
            .iter()
            .map(|r| {
                (
                    r.values[0].as_i64().expect("expected i64 value"),
                    r.values[1].as_i64().expect("expected i64 value"),
                )
            })
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
            .expect("query execution failed");
        assert_eq!(
            result_no_session.rows.len(),
            1,
            "Without session, only the persistent edge(10,20) should produce reachable(10,20)"
        );

        handler
            .close_session(&client_a)
            .expect("session close failed");
        handler
            .close_session(&client_b)
            .expect("session close failed");
    }

    #[tokio::test]
    async fn test_two_clients_only_ephemeral_base_facts() {
        // Persistent rule exists but no persistent base facts
        // Only ephemeral facts from sessions trigger the rule
        let (handler, _tmp) = handler_with_kg("multi_client_2");
        let kg = "multi_client_2";

        // Register persistent rule (no persistent edge facts)
        handler
            .query_program(
                Some(kg.to_string()),
                "+path(X, Y) <- link(X, Y)".to_string(),
            )
            .await
            .expect("query execution failed");

        let client_a = handler.create_session(kg).expect("session creation failed");
        let client_b = handler.create_session(kg).expect("session creation failed");

        // Client A: link(1,2), link(2,3)
        handler
            .session_insert_ephemeral(
                &client_a,
                "link",
                vec![
                    Tuple::new(vec![Value::Int64(1), Value::Int64(2)]),
                    Tuple::new(vec![Value::Int64(2), Value::Int64(3)]),
                ],
            )
            .expect("ephemeral insert failed");

        // Client B: link(100,200) (completely different)
        handler
            .session_insert_ephemeral(
                &client_b,
                "link",
                vec![Tuple::new(vec![Value::Int64(100), Value::Int64(200)])],
            )
            .expect("ephemeral insert failed");

        // Client A → 2 path results
        let result_a = handler
            .query_program_with_session(&client_a, "__q__(X, Y) <- path(X, Y)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result_a.rows.len(), 2);

        // Client B → 1 path result
        let result_b = handler
            .query_program_with_session(&client_b, "__q__(X, Y) <- path(X, Y)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result_b.rows.len(), 1);

        // No session → 0 results (no persistent link facts)
        let result_none = handler
            .query_program(Some(kg.to_string()), "?path(X, Y)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result_none.rows.len(), 0);

        handler
            .close_session(&client_a)
            .expect("session close failed");
        handler
            .close_session(&client_b)
            .expect("session close failed");
    }

    #[tokio::test]
    async fn test_provenance_tags_persistent_vs_ephemeral() {
        // Verify per-tuple provenance is correctly assigned
        let (handler, _tmp) = handler_with_kg("prov_tags");
        let kg = "prov_tags";

        // Persistent fact
        handler
            .query_program(Some(kg.to_string()), "+items[(1,), (2,)]".to_string())
            .await
            .expect("query execution failed");

        let sid = handler.create_session(kg).expect("session creation failed");
        // Ephemeral fact
        handler
            .session_insert_ephemeral(&sid, "items", vec![Tuple::new(vec![Value::Int64(3)])])
            .expect("ephemeral insert failed");

        let result = handler
            .query_program_with_session(&sid, "__q__(X) <- items(X)".to_string())
            .await
            .expect("query execution failed");
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

        handler.close_session(&sid).expect("session close failed");
    }

    #[tokio::test]
    async fn test_session_ephemeral_rule_augments_persistent() {
        // Client adds an ephemeral rule that extends persistent facts
        let (handler, _tmp) = handler_with_kg("eph_rule_aug");
        let kg = "eph_rule_aug";

        // Persistent base facts
        handler
            .query_program(Some(kg.to_string()), "+edge[(1, 2), (2, 3)]".to_string())
            .await
            .expect("query execution failed");

        let client_a = handler.create_session(kg).expect("session creation failed");
        let client_b = handler.create_session(kg).expect("session creation failed");

        // Client A adds an ephemeral rule: path(X,Y) <- edge(X,Y)
        let rule_a =
            crate::parser::parse_rule("path(X, Y) <- edge(X, Y)").expect("rule parsing failed");
        handler
            .session_add_rule(&client_a, rule_a, "path(X, Y) <- edge(X, Y)".to_string())
            .expect("session add rule failed");

        // Client B inserts a trivial ephemeral fact to make it "dirty"
        // (so it uses the slow path and doesn't delegate to query_program)
        handler
            .session_insert_ephemeral(&client_b, "marker", vec![Tuple::new(vec![Value::Int64(0)])])
            .expect("ephemeral insert failed");

        // Client A queries path → 2 results (from the ephemeral rule on persistent edges)
        let result_a = handler
            .query_program_with_session(&client_a, "__q__(X, Y) <- path(X, Y)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result_a.rows.len(), 2);

        // Client B queries path → 0 results (no "path" rule in client B's scope)
        let result_b = handler
            .query_program_with_session(&client_b, "__q__(X, Y) <- path(X, Y)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result_b.rows.len(), 0);

        handler
            .close_session(&client_a)
            .expect("session close failed");
        handler
            .close_session(&client_b)
            .expect("session close failed");
    }

    #[tokio::test]
    async fn test_client_close_cleans_up_ephemeral() {
        // After session close, ephemeral facts no longer affect queries
        let (handler, _tmp) = handler_with_kg("close_cleanup");
        let kg = "close_cleanup";

        handler
            .query_program(
                Some(kg.to_string()),
                "+cleanup_rule(X) <- base(X)".to_string(),
            )
            .await
            .expect("query execution failed");

        let sid = handler.create_session(kg).expect("session creation failed");
        handler
            .session_insert_ephemeral(&sid, "base", vec![Tuple::new(vec![Value::Int64(42)])])
            .expect("ephemeral insert failed");

        // Query while session is active → 1 result
        let result = handler
            .query_program_with_session(&sid, "__q__(X) <- cleanup_rule(X)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 1);

        // Close session
        handler.close_session(&sid).expect("session close failed");

        // Query without session → 0 results
        let result = handler
            .query_program(Some(kg.to_string()), "?cleanup_rule(X)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 0);
    }

    #[tokio::test]
    async fn test_many_sessions_sequential_queries() {
        // Simulate 10 "AI agent" sessions querying sequentially
        let (handler, _tmp) = handler_with_kg("many_agents");
        let kg = "many_agents";

        // Persistent base
        handler
            .query_program(Some(kg.to_string()), "+doc[(1,), (2,), (3,)]".to_string())
            .await
            .expect("query execution failed");
        handler
            .query_program(
                Some(kg.to_string()),
                "+relevant(X) <- doc(X), query_embedding(X)".to_string(),
            )
            .await
            .expect("query execution failed");

        // Create sessions and insert different query embeddings
        let mut sessions = vec![];
        for i in 0..10i64 {
            let sid = handler.create_session(kg).expect("session creation failed");
            // Each session queries for a different doc: session i queries for doc i%3+1
            handler
                .session_insert_ephemeral(
                    &sid,
                    "query_embedding",
                    vec![Tuple::new(vec![Value::Int64(i % 3 + 1)])],
                )
                .expect("ephemeral insert failed");
            sessions.push(sid);
        }

        // Query all sessions sequentially (each is isolated)
        for (i, sid) in sessions.iter().enumerate() {
            let result = handler
                .query_program_with_session(sid, "__q__(X) <- relevant(X)".to_string())
                .await
                .expect("query execution failed");
            assert_eq!(
                result.rows.len(),
                1,
                "Session {i} should see exactly 1 relevant doc"
            );
        }

        // Cleanup
        for sid in &sessions {
            handler.close_session(sid).expect("session close failed");
        }
    }

    #[tokio::test]
    async fn test_ephemeral_retract_changes_session_results() {
        // Session adds ephemeral facts, queries, retracts some, queries again
        let (handler, _tmp) = handler_with_kg("retract_changes");
        let kg = "retract_changes";

        handler
            .query_program(Some(kg.to_string()), "+visible(X) <- item(X)".to_string())
            .await
            .expect("query execution failed");

        let sid = handler.create_session(kg).expect("session creation failed");
        handler
            .session_insert_ephemeral(
                &sid,
                "item",
                vec![
                    Tuple::new(vec![Value::Int64(1)]),
                    Tuple::new(vec![Value::Int64(2)]),
                    Tuple::new(vec![Value::Int64(3)]),
                ],
            )
            .expect("ephemeral insert failed");

        // Query → 3 visible
        let result = handler
            .query_program_with_session(&sid, "__q__(X) <- visible(X)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 3);

        // Retract item(2)
        handler
            .session_retract_ephemeral(&sid, "item", vec![Tuple::new(vec![Value::Int64(2)])])
            .expect("ephemeral retract failed");

        // Query → 2 visible
        let result = handler
            .query_program_with_session(&sid, "__q__(X) <- visible(X)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 2);

        handler.close_session(&sid).expect("session close failed");
    }

    #[tokio::test]
    async fn test_session_metadata_reports_ephemeral_sources() {
        let (handler, _tmp) = handler_with_kg("meta_sources");
        let kg = "meta_sources";

        handler
            .query_program(Some(kg.to_string()), "+derived(X) <- src(X)".to_string())
            .await
            .expect("query execution failed");

        let sid = handler.create_session(kg).expect("session creation failed");
        handler
            .session_insert_ephemeral(&sid, "src", vec![Tuple::new(vec![Value::Int64(1)])])
            .expect("ephemeral insert failed");

        let result = handler
            .query_program_with_session(&sid, "__q__(X) <- derived(X)".to_string())
            .await
            .expect("query execution failed");

        // Result should have metadata about ephemeral participation
        assert!(result.metadata.is_some());
        let meta = result.metadata.expect("metadata should be present");
        assert!(meta.has_ephemeral);
        assert!(
            meta.ephemeral_sources.contains(&"src".to_string()),
            "metadata should report 'src' as ephemeral source"
        );

        handler.close_session(&sid).expect("session close failed");
    }

    // --- Notifications from mutations ---

    #[tokio::test]
    async fn test_insert_sends_notification() {
        let (handler, _tmp) = handler_with_kg("notif_ins_test");
        let mut rx = handler.subscribe_notifications();
        handler
            .query_program(
                Some("notif_ins_test".to_string()),
                "+edges[(1, 2), (3, 4)]".to_string(),
            )
            .await
            .expect("query execution failed");
        match rx.try_recv() {
            Ok(Notification::PersistentUpdate {
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
        let (handler, _tmp) = handler_with_kg("notif_del_test");
        handler
            .query_program(
                Some("notif_del_test".to_string()),
                "+edges[(1, 2), (3, 4)]".to_string(),
            )
            .await
            .expect("query execution failed");
        let mut rx = handler.subscribe_notifications();
        handler
            .query_program(
                Some("notif_del_test".to_string()),
                "-edges(1, 2)".to_string(),
            )
            .await
            .expect("query execution failed");
        match rx.try_recv() {
            Ok(Notification::PersistentUpdate {
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
        let (handler, _tmp) = make_test_handler();
        let storage = handler.get_storage();
        // Should have default KG
        assert!(storage
            .list_knowledge_graphs()
            .contains(&"default".to_string()));
    }

    #[test]
    fn test_handler_get_storage_mut() {
        let (handler, _tmp) = make_test_handler();
        let storage = handler.get_storage_mut();
        // Should be able to create a new KG
        storage
            .create_knowledge_graph("test_mut")
            .expect("knowledge graph creation failed");
        assert!(storage
            .list_knowledge_graphs()
            .contains(&"test_mut".to_string()));
    }

    #[test]
    fn test_handler_session_manager() {
        let (handler, _tmp) = make_test_handler();
        let mgr = handler.session_manager();
        assert_eq!(mgr.session_count(), 0);
    }

    #[tokio::test]
    async fn test_query_program_no_kg_selected() {
        let (storage, _tmp) = make_test_storage();
        let handler = Handler::new(storage);
        // Without an explicit KG, uses the default
        let result = handler.query_program(None, "+data[(1,)]".to_string()).await;
        // Should succeed since default KG exists
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_query_program_invalid_syntax() {
        let (handler, _tmp) = make_test_handler();
        // Invalid IQL syntax
        let result = handler
            .query_program(None, "not valid IQL !!!".to_string())
            .await;
        // Should not crash - either returns error or empty
        // (parser resilience)
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_handler_uptime() {
        let (handler, _tmp) = make_test_handler();
        // Just created, uptime should be < 2 seconds
        assert!(handler.uptime_seconds() < 2);
    }

    #[tokio::test]
    async fn test_query_program_multiline_with_mixed_comments() {
        let (handler, _tmp) = handler_with_kg("mixed_comments");
        let program = "%% header comment\n+mc_data[(1,), (2,)]\n// inline\n?mc_data(X)";
        let result = handler
            .query_program(Some("mixed_comments".to_string()), program.to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_session_insert_retract_and_query() {
        let (handler, _tmp) = handler_with_kg("sess_irt");
        let kg = "sess_irt";

        // Insert persistent base
        handler
            .query_program(Some(kg.to_string()), "+base[(10,)]".to_string())
            .await
            .expect("query execution failed");

        // Create session
        let sid = handler.create_session(kg).expect("session creation failed");

        // Add ephemeral facts
        handler
            .session_insert_ephemeral(
                &sid,
                "base",
                vec![
                    Tuple::new(vec![Value::Int64(20)]),
                    Tuple::new(vec![Value::Int64(30)]),
                ],
            )
            .expect("ephemeral insert failed");

        // Query with session → 3 results
        let result = handler
            .query_program_with_session(&sid, "__q__(X) <- base(X)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 3);

        // Retract one ephemeral fact
        handler
            .session_retract_ephemeral(&sid, "base", vec![Tuple::new(vec![Value::Int64(20)])])
            .expect("ephemeral retract failed");

        // Query again → 2 results
        let result = handler
            .query_program_with_session(&sid, "__q__(X) <- base(X)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 2);

        handler.close_session(&sid).expect("session close failed");
    }

    // === Regression tests for validate_relation_name ===

    #[test]
    fn test_validate_relation_name_valid() {
        assert!(Handler::validate_relation_name("edge").is_ok());
        assert!(Handler::validate_relation_name("my_relation").is_ok());
        assert!(Handler::validate_relation_name("a").is_ok());
    }

    #[test]
    fn test_validate_relation_name_empty() {
        assert!(Handler::validate_relation_name("").is_err());
        let err = Handler::validate_relation_name("").unwrap_err();
        assert!(err.contains("empty"), "Error should mention empty: {err}");
    }

    #[test]
    fn test_validate_relation_name_whitespace_only() {
        assert!(Handler::validate_relation_name("   ").is_err());
    }

    #[test]
    fn test_validate_relation_name_too_long() {
        let long_name = "a".repeat(257);
        let result = Handler::validate_relation_name(&long_name);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("too long"),
            "Error should mention too long: {err}"
        );
    }

    #[test]
    fn test_validate_relation_name_max_length_ok() {
        let name = "a".repeat(256);
        assert!(Handler::validate_relation_name(&name).is_ok());
    }

    #[test]
    fn test_validate_relation_name_reserved_prefix() {
        let result = Handler::validate_relation_name("__internal");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("must start with a lowercase letter"), "{err}");
    }

    // === Regression tests for max_insert_tuples in session_insert/retract_ephemeral ===

    #[test]
    fn test_session_insert_ephemeral_exceeds_max_tuples() {
        let (mut config, _tmp) = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        config.storage.performance.max_insert_tuples = 3;
        let handler = Handler::from_config(config).expect("handler creation failed");
        handler
            .get_storage()
            .ensure_knowledge_graph("test_limit")
            .expect("knowledge graph creation failed");
        let sid = handler
            .create_session("test_limit")
            .expect("session creation failed");

        // 4 tuples should exceed the limit of 3
        let tuples: Vec<Tuple> = (0..4).map(|i| Tuple::new(vec![Value::Int64(i)])).collect();
        let result = handler.session_insert_ephemeral(&sid, "rel", tuples);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Too many tuples"));
    }

    #[test]
    fn test_session_retract_ephemeral_exceeds_max_tuples() {
        let (mut config, _tmp) = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        config.storage.performance.max_insert_tuples = 2;
        let handler = Handler::from_config(config).expect("handler creation failed");
        handler
            .get_storage()
            .ensure_knowledge_graph("test_limit2")
            .expect("knowledge graph creation failed");
        let sid = handler
            .create_session("test_limit2")
            .expect("session creation failed");

        let tuples: Vec<Tuple> = (0..3).map(|i| Tuple::new(vec![Value::Int64(i)])).collect();
        let result = handler.session_retract_ephemeral(&sid, "rel", tuples);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Too many tuples"));
    }

    #[test]
    fn test_session_insert_ephemeral_within_max_tuples() {
        let (mut config, _tmp) = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        config.storage.performance.max_insert_tuples = 10;
        let handler = Handler::from_config(config).expect("handler creation failed");
        handler
            .get_storage()
            .ensure_knowledge_graph("test_ok")
            .expect("knowledge graph creation failed");
        let sid = handler
            .create_session("test_ok")
            .expect("session creation failed");

        let tuples: Vec<Tuple> = (0..5).map(|i| Tuple::new(vec![Value::Int64(i)])).collect();
        assert!(handler
            .session_insert_ephemeral(&sid, "rel", tuples)
            .is_ok());
    }

    #[test]
    fn test_session_insert_ephemeral_rejects_empty_relation() {
        let (handler, _tmp) = handler_with_kg("rel_name_test");
        let sid = handler
            .create_session("rel_name_test")
            .expect("session creation failed");
        let tuples = vec![Tuple::new(vec![Value::Int64(1)])];
        let result = handler.session_insert_ephemeral(&sid, "", tuples);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("empty"));
    }

    #[test]
    fn test_session_insert_ephemeral_rejects_reserved_prefix() {
        let (handler, _tmp) = handler_with_kg("rel_prefix_test");
        let sid = handler
            .create_session("rel_prefix_test")
            .expect("session creation failed");
        let tuples = vec![Tuple::new(vec![Value::Int64(1)])];
        let result = handler.session_insert_ephemeral(&sid, "__internal", tuples);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("must start with a lowercase letter"));
    }

    #[test]
    fn test_session_insert_invalid_session() {
        let (handler, _tmp) = make_test_handler();
        let result = handler.session_insert_ephemeral(
            &"99999".to_string(),
            "rel",
            vec![Tuple::new(vec![Value::Int64(1)])],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_session_retract_invalid_session() {
        let (handler, _tmp) = make_test_handler();
        let result = handler.session_retract_ephemeral(
            &"99999".to_string(),
            "rel",
            vec![Tuple::new(vec![Value::Int64(1)])],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_persistent_notification_serialize() {
        let notif = Notification::PersistentUpdate {
            knowledge_graph: "test".to_string(),
            relation: "edge".to_string(),
            operation: "insert".to_string(),
            count: 5,
            timestamp_ms: 1700000000000,
            session_id: None,
            seq: 1,
        };
        let json = serde_json::to_string(&notif).expect("serialization failed");
        assert!(json.contains("persistent_update"));
        assert!(json.contains("\"count\":5"));
        assert!(json.contains("\"timestamp_ms\":1700000000000"));
        // session_id should be omitted when None (skip_serializing_if)
        assert!(!json.contains("session_id"));
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
        // A variable only appearing in a negated body atom is ""unsafe" in IQL.
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
        let (handler, _tmp) = make_test_handler();
        assert_eq!(handler.total_queries(), 0);
    }

    #[test]
    fn test_handler_total_inserts_initial() {
        let (handler, _tmp) = make_test_handler();
        assert_eq!(handler.total_inserts(), 0);
    }

    #[test]
    fn test_handler_session_stats_empty() {
        let (handler, _tmp) = make_test_handler();
        let stats = handler.session_stats();
        assert_eq!(stats.total_sessions, 0);
    }

    #[tokio::test]
    async fn test_handler_query_updates_counter() {
        let (mut config, _tmp) = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        let handler = Handler::from_config(config).expect("handler creation failed");
        handler
            .query_program(Some("counter_kg".to_string()), "+data[(1,)]".to_string())
            .await
            .expect("query execution failed");
        assert!(handler.total_inserts() > 0 || handler.total_queries() > 0);
    }

    #[test]
    fn test_handler_debug_query() {
        let (handler, _tmp) = make_test_handler();
        {
            let storage = handler.get_storage_mut();
            storage
                .create_knowledge_graph("debug_h_kg")
                .expect("knowledge graph creation failed");
        }
        let trace = handler.debug_query(
            Some("debug_h_kg".to_string()),
            "result(X, Y) <- edge(X, Y)".to_string(),
        );
        assert!(trace.is_ok());
    }

    #[test]
    fn test_handler_subscribe_notifications() {
        let (handler, _tmp) = make_test_handler();
        let rx = handler.subscribe_notifications();
        // Should create a valid receiver without errors
        drop(rx);
    }

    #[tokio::test]
    async fn test_handler_query_select_all_tuples() {
        let (mut config, _tmp) = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        let handler = Handler::from_config(config).expect("handler creation failed");
        handler
            .query_program(
                Some("sel_kg".to_string()),
                "+items[(1, 10), (2, 20)]".to_string(),
            )
            .await
            .expect("query execution failed");
        let result = handler
            .query_program(Some("sel_kg".to_string()), "?items(X, Y)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_handler_query_with_rule() {
        let (mut config, _tmp) = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        let handler = Handler::from_config(config).expect("handler creation failed");
        handler
            .query_program(
                Some("rule_q_kg".to_string()),
                "+edge[(1, 2), (2, 3)]".to_string(),
            )
            .await
            .expect("query execution failed");
        // Define a persistent rule
        handler
            .query_program(
                Some("rule_q_kg".to_string()),
                "+path(X, Y) <- edge(X, Y)".to_string(),
            )
            .await
            .expect("query execution failed");
        // Query the derived relation
        let result = handler
            .query_program(Some("rule_q_kg".to_string()), "?path(X, Y)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_handler_uptime_seconds() {
        let (handler, _tmp) = make_test_handler();
        // Just created, uptime should be 0 or very small
        assert!(handler.uptime_seconds() < 2);
    }

    #[tokio::test]
    async fn test_handler_total_queries_after_query() {
        let (mut config, _tmp) = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        let handler = Handler::from_config(config).expect("handler creation failed");
        handler
            .query_program(Some("tq_kg".to_string()), "+data[(1,)]".to_string())
            .await
            .expect("query execution failed");
        assert!(handler.total_queries() > 0 || handler.total_inserts() > 0);
    }

    #[test]
    fn test_handler_close_session_invalid() {
        let (handler, _tmp) = make_test_handler();
        let result = handler.close_session(&"999999".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_handler_validate_tuples_no_schema() {
        let (handler, _tmp) = make_test_handler();
        {
            let storage = handler.get_storage_mut();
            storage
                .create_knowledge_graph("val_kg")
                .expect("knowledge graph creation failed");
        }
        // With no schema defined, validation should pass
        let tuples = vec![Tuple::new(vec![Value::Int32(1)])];
        let result = handler.validate_tuples_against_schema("val_kg", "test_rel", &tuples);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handler_notify_persistent_update() {
        let (handler, _tmp) = make_test_handler();
        // Should not panic - fire and forget notification
        handler.notify_persistent_update("kg", "rel", "insert", 5);
    }

    #[tokio::test]
    async fn test_handler_query_program_insert_and_query() {
        let (mut config, _tmp) = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        let handler = Handler::from_config(config).expect("handler creation failed");
        // Insert data
        handler
            .query_program(
                Some("qp_iq_kg".to_string()),
                "+scores[(1, 100), (2, 200)]".to_string(),
            )
            .await
            .expect("query execution failed");
        // Query it back
        let result = handler
            .query_program(Some("qp_iq_kg".to_string()), "?scores(X, Y)".to_string())
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_handler_query_program_nonexistent_kg() {
        let (handler, _tmp) = make_test_handler();
        let result = handler
            .query_program(
                Some("nonexistent_kg_xyz".to_string()),
                "?data(X)".to_string(),
            )
            .await;
        assert!(result.is_err());
    }

    // --- compare_wire_values / sort tests ---

    #[test]
    fn test_compare_wire_values_same_type() {
        use std::cmp::Ordering;
        let a = WireValue::Int64(1);
        let b = WireValue::Int64(2);
        assert_eq!(compare_wire_values(Some(&a), Some(&b)), Ordering::Less);
    }

    #[test]
    fn test_compare_wire_values_null_ordering() {
        use std::cmp::Ordering;
        let v = WireValue::Int64(1);
        let n = WireValue::Null;
        assert_eq!(compare_wire_values(Some(&n), Some(&v)), Ordering::Less);
        assert_eq!(compare_wire_values(Some(&v), Some(&n)), Ordering::Greater);
        assert_eq!(compare_wire_values(None, Some(&v)), Ordering::Less);
    }

    #[test]
    fn test_compare_wire_values_cross_type_stable() {
        use std::cmp::Ordering;
        let int_val = WireValue::Int64(100);
        let str_val = WireValue::String("hello".to_string());
        // Int (rank 3) < String (rank 5) → Less
        assert_eq!(
            compare_wire_values(Some(&int_val), Some(&str_val)),
            Ordering::Less
        );
        assert_eq!(
            compare_wire_values(Some(&str_val), Some(&int_val)),
            Ordering::Greater
        );
    }

    #[test]
    fn test_compare_wire_values_cross_numeric() {
        use std::cmp::Ordering;
        let i = WireValue::Int64(2);
        let f = WireValue::Float64(1.5);
        assert_eq!(compare_wire_values(Some(&i), Some(&f)), Ordering::Greater);
    }

    #[test]
    fn test_sort_rows_empty_order() {
        let rows = vec![
            WireTuple::new(vec![WireValue::Int64(2)]),
            WireTuple::new(vec![WireValue::Int64(1)]),
        ];
        let sorted = sort_rows(rows.clone(), &[]);
        assert_eq!(sorted.len(), 2);
        // No sorting applied - same order
        assert_eq!(sorted[0].values[0], WireValue::Int64(2));
    }

    #[test]
    fn test_sort_rows_single_col_asc() {
        let rows = vec![
            WireTuple::new(vec![WireValue::Int64(3)]),
            WireTuple::new(vec![WireValue::Int64(1)]),
            WireTuple::new(vec![WireValue::Int64(2)]),
        ];
        let sorted = sort_rows(rows, &[(0, SortDirection::Asc)]);
        assert_eq!(sorted[0].values[0], WireValue::Int64(1));
        assert_eq!(sorted[1].values[0], WireValue::Int64(2));
        assert_eq!(sorted[2].values[0], WireValue::Int64(3));
    }

    #[test]
    fn test_sort_rows_single_col_desc() {
        let rows = vec![
            WireTuple::new(vec![WireValue::Int64(1)]),
            WireTuple::new(vec![WireValue::Int64(3)]),
            WireTuple::new(vec![WireValue::Int64(2)]),
        ];
        let sorted = sort_rows(rows, &[(0, SortDirection::Desc)]);
        assert_eq!(sorted[0].values[0], WireValue::Int64(3));
        assert_eq!(sorted[1].values[0], WireValue::Int64(2));
        assert_eq!(sorted[2].values[0], WireValue::Int64(1));
    }

    // --- apply_pagination tests ---

    fn make_int_rows(n: usize) -> Vec<WireTuple> {
        (1..=n)
            .map(|i| WireTuple::new(vec![WireValue::Int64(i as i64)]))
            .collect()
    }

    #[test]
    fn test_apply_pagination_no_limit() {
        let rows = make_int_rows(5);
        let result = apply_pagination(rows.clone(), None, None);
        assert_eq!(result.len(), 5);
        assert_eq!(result, rows);
    }

    #[test]
    fn test_apply_pagination_with_limit() {
        let rows = make_int_rows(5);
        let result = apply_pagination(rows, Some(2), None);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], WireValue::Int64(1));
        assert_eq!(result[1].values[0], WireValue::Int64(2));
    }

    #[test]
    fn test_apply_pagination_with_offset() {
        let rows = make_int_rows(5);
        let result = apply_pagination(rows, None, Some(2));
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values[0], WireValue::Int64(3));
        assert_eq!(result[1].values[0], WireValue::Int64(4));
        assert_eq!(result[2].values[0], WireValue::Int64(5));
    }

    #[test]
    fn test_apply_pagination_limit_exceeds() {
        let rows = make_int_rows(3);
        let result = apply_pagination(rows, Some(10), None);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_apply_pagination_offset_exceeds() {
        let rows = make_int_rows(3);
        let result = apply_pagination(rows, None, Some(10));
        assert!(result.is_empty());
    }

    #[test]
    fn test_apply_pagination_limit_zero() {
        let rows = make_int_rows(5);
        let result = apply_pagination(rows, Some(0), None);
        assert!(result.is_empty());
    }

    // --- execute_program session persistence tests ---

    #[tokio::test]
    async fn test_execute_program_session_rule_persists() {
        let (handler, _tmp) = handler_with_kg("exec_prog_sr");

        // Insert base data
        handler
            .execute_program(
                None,
                Some("exec_prog_sr".to_string()),
                "+edge[(1,2), (2,3)]".to_string(),
                None,
            )
            .await
            .expect("query execution failed");

        // Create a session
        let sid = handler
            .create_session("exec_prog_sr")
            .expect("session creation failed");

        // Add a session rule
        let result = handler
            .execute_program(
                Some(&sid),
                None,
                "path(X, Y) <- edge(X, Y)".to_string(),
                None,
            )
            .await
            .expect("query execution failed");
        assert!(result.rows[0].values[0]
            .as_str()
            .expect("operation should succeed")
            .contains("Session rule added"));

        // Verify session is now dirty (has rules)
        let is_clean = handler
            .session_manager()
            .is_session_clean(&sid)
            .expect("session state check failed");
        assert!(!is_clean, "Session should be dirty after adding rule");

        // Query using the session rule - should return results
        let result = handler
            .execute_program(Some(&sid), None, "?path(X, Y)".to_string(), None)
            .await
            .expect("query execution failed");
        assert_eq!(result.rows.len(), 2, "Should have 2 rows from path query");
    }

    #[tokio::test]
    async fn test_execute_program_touches_session_on_non_query() {
        let (mut config, tmp) = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        let storage = StorageEngine::new(config).expect("storage creation failed");
        let session_config = SessionConfig {
            idle_timeout_secs: 1,
            ..SessionConfig::default()
        };
        let handler = Handler::with_session_config(storage, session_config);
        handler
            .get_storage()
            .ensure_knowledge_graph("sess_touch")
            .expect("knowledge graph creation failed");

        let sid = handler
            .create_session("sess_touch")
            .expect("session creation failed");

        tokio::time::sleep(Duration::from_secs(2)).await;

        handler
            .execute_program(Some(&sid), None, "+edge[(1,2)]".to_string(), None)
            .await
            .expect("query execution failed");

        let reaped = handler.session_manager().reap_expired();
        assert_eq!(reaped, 0, "Session should remain alive after execute");
        assert!(handler.session_manager().has_session(&sid));

        drop(tmp);
    }

    // --- Parse-all-first validation tests ---

    #[tokio::test]
    async fn test_multi_statement_parse_error_rejects_entire_program() {
        let (handler, _tmp) = handler_with_kg("parse_reject");
        // Program: valid insert, then invalid line, then another insert
        // The invalid line should cause the ENTIRE program to be rejected
        let program = "+edge[(1,2)]\n+edge(bad syntax\n+edge[(3,4)]".to_string();
        let result = handler
            .query_program(Some("parse_reject".to_string()), program)
            .await;
        assert!(result.is_err(), "Should reject program with parse error");
        let err = result.unwrap_err();
        assert!(
            err.starts_with(VALIDATION_ERROR_PREFIX),
            "Error should contain validation prefix"
        );
    }

    #[tokio::test]
    async fn test_no_partial_state_on_rejection() {
        let (handler, _tmp) = handler_with_kg("no_partial");
        // Try to execute a program with a parse error
        let program = "+edge[(1,2)]\n+edge(bad\n+edge[(3,4)]".to_string();
        let _ = handler
            .query_program(Some("no_partial".to_string()), program)
            .await;
        // The insert should NOT have been committed
        let query_result = handler
            .query_program(Some("no_partial".to_string()), "?edge(X, Y)".to_string())
            .await
            .expect("query execution failed");
        assert!(
            query_result.rows.is_empty(),
            "No data should exist after rejected program"
        );
    }

    #[tokio::test]
    async fn test_multi_statement_all_valid_executes() {
        let (handler, _tmp) = handler_with_kg("all_valid");
        let program = "+edge[(1,2)]\n+edge[(3,4)]".to_string();
        let result = handler
            .query_program(Some("all_valid".to_string()), program)
            .await;
        assert!(result.is_ok(), "All-valid program should succeed");
    }

    #[tokio::test]
    async fn test_parse_error_reports_line_numbers() {
        let (handler, _tmp) = handler_with_kg("line_nums");
        let program = "+edge[(1,2)]\n+edge(bad syntax".to_string();
        let result = handler
            .query_program(Some("line_nums".to_string()), program)
            .await;
        let err = result.unwrap_err();
        let json_str = err
            .strip_prefix(VALIDATION_ERROR_PREFIX)
            .expect("expected validation error prefix");
        let errors: Vec<ValidationError> =
            serde_json::from_str(json_str).expect("deserialization failed");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].line, 2, "Error should be on line 2");
        assert_eq!(
            errors[0].statement_index, 1,
            "Error should be statement index 1"
        );
    }

    #[tokio::test]
    async fn test_multiple_parse_errors_all_reported() {
        let (handler, _tmp) = handler_with_kg("multi_err");
        let program = "+edge(bad1\n+edge[(1,2)]\n+edge(bad2".to_string();
        let result = handler
            .query_program(Some("multi_err".to_string()), program)
            .await;
        let err = result.unwrap_err();
        let json_str = err
            .strip_prefix(VALIDATION_ERROR_PREFIX)
            .expect("expected validation error prefix");
        let errors: Vec<ValidationError> =
            serde_json::from_str(json_str).expect("deserialization failed");
        assert_eq!(errors.len(), 2, "Should report both parse errors");
        assert_eq!(errors[0].line, 1);
        assert_eq!(errors[1].line, 3);
    }

    // === Multi-line statement (continuation line) tests ===

    #[test]
    fn test_join_continuation_lines_from_strip_comments() {
        // Exact input that strip_comments produces for the multiline test
        let stripped = strip_comments(
            "+edge[(1,2)]\n+edge[(2,3)]\nreachable(X, Y) <-\n  edge(X, Y).\n?reachable(X, Y)",
        );
        let joined = join_continuation_lines(&stripped);
        assert_eq!(
            joined, "+edge[(1,2)]\n+edge[(2,3)]\nreachable(X, Y) <- edge(X, Y).\n?reachable(X, Y)",
            "Joined output: {joined:?}",
        );
    }

    #[test]
    fn test_join_continuation_lines_basic() {
        let input = "reachable(X, Y) <-\n  edge(X, Y).";
        let result = join_continuation_lines(input);
        assert_eq!(result, "reachable(X, Y) <- edge(X, Y).");
    }

    #[test]
    fn test_join_continuation_lines_multiple_continuations() {
        let input = "reachable(X, Z) <-\n  reachable(X, Y),\n  edge(Y, Z).";
        let result = join_continuation_lines(input);
        assert_eq!(result, "reachable(X, Z) <- reachable(X, Y), edge(Y, Z).");
    }

    #[test]
    fn test_join_continuation_lines_no_continuations() {
        let input = "+edge[(1,2)]\n+edge[(3,4)]";
        let result = join_continuation_lines(input);
        assert_eq!(result, "+edge[(1,2)]\n+edge[(3,4)]");
    }

    #[test]
    fn test_join_continuation_lines_mixed() {
        let input = "+edge[(1,2)]\nreachable(X, Y) <-\n  edge(X, Y).\n?reachable(X, Y)";
        let result = join_continuation_lines(input);
        assert_eq!(
            result,
            "+edge[(1,2)]\nreachable(X, Y) <- edge(X, Y).\n?reachable(X, Y)"
        );
    }

    #[test]
    fn test_join_continuation_lines_empty_lines_preserved() {
        let input = "+edge[(1,2)]\n\n+edge[(3,4)]";
        let result = join_continuation_lines(input);
        assert_eq!(result, "+edge[(1,2)]\n\n+edge[(3,4)]");
    }

    #[test]
    fn test_join_continuation_lines_tab_indent() {
        let input = "reachable(X, Y) <-\n\tedge(X, Y).";
        let result = join_continuation_lines(input);
        assert_eq!(result, "reachable(X, Y) <- edge(X, Y).");
    }

    #[tokio::test]
    async fn test_multiline_rule_executes_correctly() {
        let (handler, _tmp) = handler_with_kg("multiline");
        // Insert data, define a multi-line rule, then query
        let program =
            "+edge[(1,2)]\n+edge[(2,3)]\nreachable(X, Y) <-\n  edge(X, Y)\n?reachable(X, Y)"
                .to_string();
        let result = handler
            .query_program(Some("multiline".to_string()), program)
            .await;
        assert!(
            result.is_ok(),
            "Multi-line rule program should succeed: {}",
            result.as_ref().unwrap_err()
        );
        let qr = result.expect("operation should succeed");
        assert_eq!(qr.rows.len(), 2, "Should return 2 reachable pairs");
    }

    #[tokio::test]
    async fn test_multiline_rule_with_multiple_body_atoms() {
        let (handler, _tmp) = handler_with_kg("multiline2");
        let program =
            "+edge[(1,2)]\n+edge[(2,3)]\npath(X, Z) <-\n  edge(X, Y),\n  edge(Y, Z)\n?path(X, Z)"
                .to_string();
        let result = handler
            .query_program(Some("multiline2".to_string()), program)
            .await;
        assert!(
            result.is_ok(),
            "Multi-line rule with multiple body atoms should succeed: {}",
            result.as_ref().unwrap_err()
        );
        let qr = result.expect("operation should succeed");
        assert_eq!(qr.rows.len(), 1, "Should return path(1,3)");
    }

    // === Regression tests for production readiness fixes ===

    /// P0-6 regression: try_get_storage returns None when write lock is held.
    /// Ensures the health check can detect lock contention.
    #[test]
    fn test_try_get_storage_returns_none_when_write_locked() {
        let (handler, _tmp) = make_test_handler();
        let handler = Arc::new(handler);

        // Hold a write lock from another thread for 2 seconds
        let h2 = Arc::clone(&handler);
        let lock_thread = std::thread::spawn(move || {
            let _guard = h2.get_storage_mut();
            std::thread::sleep(Duration::from_secs(2));
        });

        // Give the thread time to acquire the write lock
        std::thread::sleep(Duration::from_millis(50));

        // try_get_storage should fail quickly (10ms timeout)
        let result = handler.try_get_storage(Duration::from_millis(10));
        assert!(
            result.is_none(),
            "try_get_storage should return None when write lock is held"
        );

        lock_thread.join().expect("thread join failed");
    }

    /// P0-6 regression: Session IDs are unique UUIDs, not sequential integers.
    /// Prevents session enumeration attacks.
    #[test]
    fn test_session_ids_are_unique_uuids() {
        let (mut config, _tmp) = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        let handler = Handler::from_config(config).expect("handler creation failed");
        let id1 = handler
            .create_session("default")
            .expect("session creation failed");
        let id2 = handler
            .create_session("default")
            .expect("session creation failed");

        assert_ne!(id1, id2, "Session IDs must be unique");
        // UUID v4 format: 8-4-4-4-12 = 36 chars
        assert_eq!(id1.len(), 36, "Session ID should be UUID format (36 chars)");
        assert_eq!(id2.len(), 36, "Session ID should be UUID format (36 chars)");
        assert!(id1.contains('-'), "Session ID should contain UUID dashes");
        // Ensure not sequential
        let id3 = handler
            .create_session("default")
            .expect("session creation failed");
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    /// Shutdown regression: handler.shutdown() flushes data so it survives restart.
    #[tokio::test]
    async fn test_handler_shutdown_flushes_data() {
        let tmp = tempfile::tempdir().expect("failed to create temp dir");
        let data_dir = tmp.path().to_path_buf();

        // Phase 1: Create handler, insert data, shutdown
        {
            let mut config = Config::default();
            config.storage.data_dir = data_dir.clone();
            config.storage.auto_create_knowledge_graphs = true;
            let handler = Handler::from_config(config).expect("handler creation failed");
            handler
                .query_program(
                    Some("shutdown_kg".to_string()),
                    "+persist_data[(1, 2), (3, 4)]".to_string(),
                )
                .await
                .expect("query execution failed");
            handler.shutdown();
        }

        // Phase 2: Reopen from same data_dir, verify data survived
        {
            let mut config = Config::default();
            config.storage.data_dir = data_dir;
            let handler = Handler::from_config(config).expect("handler creation failed");
            let result = handler
                .query_program(
                    Some("shutdown_kg".to_string()),
                    "?persist_data(X, Y)".to_string(),
                )
                .await
                .expect("query execution failed");
            assert_eq!(
                result.rows.len(),
                2,
                "Data should survive shutdown + restart"
            );
        }
    }

    // === Column naming tests ===

    #[test]
    fn test_extract_column_names_from_simple_query() {
        let names = extract_column_names_from_query("__query__(X, Y) <- edge(X, Y)", 2);
        assert_eq!(names, vec!["X", "Y"]);
    }

    #[test]
    fn test_extract_column_names_with_multiple_vars() {
        let names = extract_column_names_from_query(
            "__query__(Name, Age, City) <- person(Name, Age, City)",
            3,
        );
        assert_eq!(names, vec!["Name", "Age", "City"]);
    }

    #[test]
    fn test_extract_column_names_parse_failure_fallback() {
        let names = extract_column_names_from_query("this is not valid IQL!!!", 3);
        assert_eq!(names, vec!["col0", "col1", "col2"]);
    }

    #[test]
    fn test_extract_column_names_arity_mismatch_fallback() {
        // Query has 2 head vars but we ask for 3 columns
        let names = extract_column_names_from_query("__query__(X, Y) <- edge(X, Y)", 3);
        assert_eq!(names, vec!["col0", "col1", "col2"]);
    }

    #[test]
    fn test_find_query_source_relation_shorthand() {
        let rel = find_query_source_relation("__query__(X, Y) <- edge(X, Y)");
        assert_eq!(rel, Some("edge".to_string()));
    }

    #[test]
    fn test_find_query_source_relation_named() {
        let rel = find_query_source_relation("path(X, Y) <- edge(X, Y)");
        assert_eq!(rel, Some("path".to_string()));
    }

    #[test]
    fn test_find_query_source_relation_parse_failure() {
        let rel = find_query_source_relation("not valid!!!");
        assert_eq!(rel, None);
    }

    #[test]
    fn test_extract_column_names_constants_in_rule() {
        let names = extract_column_names_from_query("__query__(X, 42) <- edge(X, 42)", 2);
        assert_eq!(names[0], "X");
        assert_eq!(names[1], "42");
    }

    #[test]
    fn test_extract_column_names_constant_binding_resolved() {
        // Parser desugars ?tc(1, X) into __query__(_c0, X) <- tc(_c0, X), _c0 = 1
        // The _c0 should be resolved back to "1"
        let names = extract_column_names_from_query("?tc(1, X)", 2);
        assert_eq!(names, vec!["1", "X"]);
    }

    #[test]
    fn test_extract_column_names_placeholder_resolved() {
        // Parser desugars ?people(Id, _, 25) into
        // __query__(Id, _p1, _c2) <- people(Id, _p1, _c2), _c2 = 25
        // _p1 should resolve to "_", _c2 to "25"
        let names = extract_column_names_from_query("?people(Id, _, 25)", 3);
        assert_eq!(names, vec!["Id", "_", "25"]);
    }

    #[test]
    fn test_extract_column_names_all_constants() {
        let names = extract_column_names_from_query("?facts(1, 2)", 2);
        assert_eq!(names, vec!["1", "2"]);
    }

    // ── Column header priority integration tests ────────────────────────────

    #[tokio::test]
    async fn test_schema_relation_uses_schema_column_names() {
        let (handler, _tmp) = make_test_handler();
        // Define schema, insert data, query with different variable names
        handler
            .query_program(
                None,
                "+product(product_id: int, name: string, price: float)".to_string(),
            )
            .await
            .expect("schema registration failed");
        handler
            .query_program(None, "+product[(1, \"Widget\", 9.99)]".to_string())
            .await
            .expect("insert failed");
        let result = handler
            .query_program(None, "?product(X, Y, Z)".to_string())
            .await
            .expect("query failed");
        let col_names: Vec<String> = result.schema.iter().map(|c| c.name.clone()).collect();
        assert_eq!(
            col_names,
            vec!["product_id", "name", "price"],
            "schema-defined relation should use schema column names, not query variables"
        );
    }

    #[tokio::test]
    async fn test_no_schema_relation_uses_query_variable_names() {
        let (handler, _tmp) = make_test_handler();
        // Insert without schema, query with variables
        handler
            .query_program(None, "+edge[(1, 2), (2, 3)]".to_string())
            .await
            .expect("insert failed");
        let result = handler
            .query_program(None, "?edge(From, To)".to_string())
            .await
            .expect("query failed");
        let col_names: Vec<String> = result.schema.iter().map(|c| c.name.clone()).collect();
        assert_eq!(
            col_names,
            vec!["From", "To"],
            "no-schema relation should use query variable names"
        );
    }

    #[tokio::test]
    async fn test_derived_relation_uses_query_variable_names() {
        let (handler, _tmp) = make_test_handler();
        handler
            .query_program(None, "+edge[(1, 2), (2, 3)]".to_string())
            .await
            .expect("insert failed");
        handler
            .query_program(
                None,
                "+path(A, B) <- edge(A, B)\n+path(A, C) <- path(A, B), edge(B, C)".to_string(),
            )
            .await
            .expect("rule registration failed");
        let result = handler
            .query_program(None, "?path(Source, Dest)".to_string())
            .await
            .expect("query failed");
        let col_names: Vec<String> = result.schema.iter().map(|c| c.name.clone()).collect();
        assert_eq!(
            col_names,
            vec!["Source", "Dest"],
            "derived relation (rule) should use query variable names"
        );
    }

    #[tokio::test]
    async fn test_schema_relation_with_constant_in_query() {
        let (handler, _tmp) = make_test_handler();
        handler
            .query_program(None, "+item(item_id: int, label: string)".to_string())
            .await
            .expect("schema registration failed");
        handler
            .query_program(None, "+item[(1, \"alpha\"), (2, \"beta\")]".to_string())
            .await
            .expect("insert failed");
        let result = handler
            .query_program(None, "?item(1, Name)".to_string())
            .await
            .expect("query failed");
        let col_names: Vec<String> = result.schema.iter().map(|c| c.name.clone()).collect();
        assert_eq!(
            col_names,
            vec!["item_id", "label"],
            "schema names should be used even when query has constants"
        );
    }

    // ── Meta-command handler tests ──────────────────────────────────────────

    #[tokio::test]
    async fn test_handler_meta_rel_list() {
        let (handler, _tmp) = make_test_handler();
        handler
            .query_program(None, "+edge[(1, 2), (3, 4)]".to_string())
            .await
            .expect("query execution failed");
        let result = handler
            .query_program(None, ".rel".to_string())
            .await
            .expect("query execution failed");
        let text = result
            .rows
            .iter()
            .map(|r| {
                r.values[0]
                    .as_str()
                    .expect("expected string value")
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("edge"), "should list the edge relation");
        assert!(text.contains("arity:"), "should show arity");
        assert!(text.contains("columns:"), "should show columns");
        assert!(text.contains("tuples:"), "should show tuple count");
        // Verify typed format: column entries should contain ":"
        // e.g. "col0: any" in the columns list
        assert!(
            text.contains(": any") || text.contains(": int") || text.contains(": string"),
            "columns should include type annotations"
        );
    }

    #[tokio::test]
    async fn test_handler_meta_rel_list_with_typed_schema() {
        let (handler, _tmp) = make_test_handler();
        // Declare a typed schema, then insert data
        handler
            .query_program(None, "+person(id: int, name: string)".to_string())
            .await
            .expect("query execution failed");
        handler
            .query_program(None, "+person[(1, \"alice\")]".to_string())
            .await
            .expect("query execution failed");
        let result = handler
            .query_program(None, ".rel".to_string())
            .await
            .expect("query execution failed");
        let text = result
            .rows
            .iter()
            .map(|r| {
                r.values[0]
                    .as_str()
                    .expect("expected string value")
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains("id: int"),
            "should show typed column 'id: int', got: {text}"
        );
        assert!(
            text.contains("name: string"),
            "should show typed column 'name: string', got: {text}"
        );
    }

    #[tokio::test]
    async fn test_handler_meta_rule_list() {
        let (handler, _tmp) = make_test_handler();
        handler
            .query_program(None, "+edge[(1, 2)]".to_string())
            .await
            .expect("query execution failed");
        handler
            .query_program(
                None,
                "+path(X, Y) <- edge(X, Y)\n+path(X, Z) <- edge(X, Y), path(Y, Z)".to_string(),
            )
            .await
            .expect("query execution failed");
        let result = handler
            .query_program(None, ".rule list".to_string())
            .await
            .expect("query execution failed");
        let text = result
            .rows
            .iter()
            .map(|r| {
                r.values[0]
                    .as_str()
                    .expect("expected string value")
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("path"), "should list the 'path' rule");
        assert!(
            text.contains("2 clause"),
            "should show 2 clauses, got: {text}"
        );
    }

    #[tokio::test]
    async fn test_handler_meta_rule_def() {
        let (handler, _tmp) = make_test_handler();
        handler
            .query_program(None, "+edge[(1, 2)]".to_string())
            .await
            .expect("query execution failed");
        handler
            .query_program(
                None,
                "+path(X, Y) <- edge(X, Y)\n+path(X, Z) <- edge(X, Y), path(Y, Z)".to_string(),
            )
            .await
            .expect("query execution failed");
        let result = handler
            .query_program(None, ".rule def path".to_string())
            .await
            .expect("query execution failed");
        let text = result
            .rows
            .iter()
            .map(|r| {
                r.values[0]
                    .as_str()
                    .expect("expected string value")
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("path"), "definition should mention 'path'");
        assert!(
            text.contains("edge"),
            "definition should reference 'edge' relation"
        );
        assert!(text.contains("<-"), "definition should show rule arrows");
    }

    #[tokio::test]
    async fn test_handler_meta_kg_list() {
        let (handler, _tmp) = make_test_handler();
        let result = handler
            .query_program(None, ".kg list".to_string())
            .await
            .expect("query execution failed");
        let text = result
            .rows
            .iter()
            .map(|r| {
                r.values[0]
                    .as_str()
                    .expect("expected string value")
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains("default"),
            "should list the default KG, got: {text}"
        );
    }

    #[tokio::test]
    async fn test_handler_meta_debug() {
        let (handler, _tmp) = make_test_handler();
        handler
            .query_program(None, "+edge[(1, 2), (3, 4)]".to_string())
            .await
            .expect("query execution failed");
        let result = handler
            .query_program(None, ".debug ?edge(X, Y)".to_string())
            .await
            .expect("query execution failed");
        let text = result
            .rows
            .iter()
            .map(|r| {
                r.values[0]
                    .as_str()
                    .expect("expected string value")
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        // Debug should produce some plan output
        assert!(!text.is_empty(), "debug should produce non-empty output");
    }
}

/// Recursively extract variable names from a term.
fn extract_term_vars(term: &Term, vars: &mut Vec<String>) {
    match term {
        Term::Variable(v) if !vars.contains(v) => {
            vars.push(v.clone());
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
        // Constants, placeholders, aggregates, vectors - no variables to extract
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
        // Constants - no variables
        crate::ast::ArithExpr::Constant(_) | crate::ast::ArithExpr::FloatConstant(_) => {}
    }
}

/// Extract variables from a body predicate and add to `head_vars`
/// Used for Cartesian product queries like ?- foo(X), bar(Y).
/// Sorting and offsets must see every row, so such queries run uncapped and
/// are cut by [`cap_rows`] afterwards.
fn needs_full_result(order_by: &[(usize, SortDirection)], offset: Option<usize>) -> bool {
    !order_by.is_empty() || offset.is_some_and(|n| n > 0)
}

/// Cuts `rows` to `max_rows` (0 = no limit), reporting whether any were dropped.
fn cap_rows(mut rows: Vec<WireTuple>, max_rows: usize) -> (Vec<WireTuple>, bool) {
    let cut = max_rows > 0 && rows.len() > max_rows;
    if cut {
        rows.truncate(max_rows);
    }
    (rows, cut)
}

/// Apply offset and limit pagination to result rows.
fn apply_pagination(
    rows: Vec<WireTuple>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Vec<WireTuple> {
    let start = offset.unwrap_or(0);
    if start >= rows.len() {
        return vec![];
    }
    let remaining = &rows[start..];
    match limit {
        Some(n) => remaining.iter().take(n).cloned().collect(),
        None => remaining.to_vec(),
    }
}

/// Sort result rows by the given column indices and directions.
/// Returns the rows unchanged if `order_by` is empty.
fn sort_rows(mut rows: Vec<WireTuple>, order_by: &[(usize, SortDirection)]) -> Vec<WireTuple> {
    if order_by.is_empty() {
        return rows;
    }
    rows.sort_by(|a, b| {
        for &(col_idx, dir) in order_by {
            let va = a.values.get(col_idx);
            let vb = b.values.get(col_idx);
            let cmp = compare_wire_values(va, vb);
            let cmp = match dir {
                SortDirection::Asc => cmp,
                SortDirection::Desc => cmp.reverse(),
            };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });
    rows
}

/// Compare two optional WireValues for sorting purposes.
fn compare_wire_values(a: Option<&WireValue>, b: Option<&WireValue>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(va), Some(vb)) => match (va, vb) {
            (WireValue::Int64(a), WireValue::Int64(b)) => a.cmp(b),
            (WireValue::Int32(a), WireValue::Int32(b)) => a.cmp(b),
            (WireValue::Float64(a), WireValue::Float64(b)) => {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            }
            (WireValue::String(a), WireValue::String(b)) => a.cmp(b),
            (WireValue::Bool(a), WireValue::Bool(b)) => a.cmp(b),
            (WireValue::Timestamp(a), WireValue::Timestamp(b)) => a.cmp(b),
            (WireValue::Null, WireValue::Null) => std::cmp::Ordering::Equal,
            (WireValue::Null, _) => std::cmp::Ordering::Less,
            (_, WireValue::Null) => std::cmp::Ordering::Greater,
            // Cross-type numeric comparison
            (WireValue::Int64(a), WireValue::Float64(b)) => (*a as f64)
                .partial_cmp(b)
                .unwrap_or(std::cmp::Ordering::Equal),
            (WireValue::Float64(a), WireValue::Int64(b)) => a
                .partial_cmp(&(*b as f64))
                .unwrap_or(std::cmp::Ordering::Equal),
            // Cross-type: use type discriminant for stable ordering
            _ => wire_value_type_rank(va).cmp(&wire_value_type_rank(vb)),
        },
    }
}

/// Assign a rank to each WireValue variant for stable cross-type ordering.
fn wire_value_type_rank(v: &WireValue) -> u8 {
    match v {
        WireValue::Null => 0,
        WireValue::Bool(_) => 1,
        WireValue::Int32(_) => 2,
        WireValue::Int64(_) => 3,
        WireValue::Float64(_) => 4,
        WireValue::String(_) => 5,
        WireValue::Timestamp(_) => 6,
        WireValue::Vector(_) | WireValue::VectorInt8(_) => 7,
        WireValue::Bytes(_) => 8,
    }
}

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
            // in IQL - it cannot be safely projected into the head.
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

/// Extract the query relation name from an IQL query string.
///
/// Handles both `?relation(X, Y)` shorthand and `__query__(X, Y) <- relation(X, Y)` forms.
fn extract_query_relation(query: &str) -> Option<String> {
    let trimmed = query.trim();

    // Handle ?shorthand: "?relation(X, Y)" -> "relation"
    if let Some(rest) = trimmed.strip_prefix('?') {
        if let Some(paren) = rest.find('(') {
            return Some(rest[..paren].trim().to_string());
        }
        return Some(rest.trim().to_string());
    }

    // Handle rule form: "name(X, Y) <- body(...)" -> look at body for actual relation
    // For the .why command, the relation is the head of the query rule
    if let Some(arrow) = trimmed.find("<-") {
        let head = trimmed[..arrow].trim();
        if let Some(paren) = head.find('(') {
            let name = head[..paren].trim();
            // Skip internal wrapper names like __query__, __cond_del_query__, etc.
            if name.starts_with("__") && name.ends_with("__") {
                // Look at the first body atom instead
                let body = trimmed[arrow + 2..].trim();
                if let Some(bp) = body.find('(') {
                    return Some(body[..bp].trim().to_string());
                }
            }
            return Some(name.to_string());
        }
    }

    None
}

/// Parse a `.why_not` target like `relation(1, 2, "hello")` into a relation name and tuple.
fn parse_why_not_target(input: &str) -> Result<(String, crate::value::Tuple), String> {
    let trimmed = input.trim();
    let paren_start = trimmed
        .find('(')
        .ok_or_else(|| format!("Expected format: relation(val1, val2, ...), got: {trimmed}"))?;
    let paren_end = trimmed
        .rfind(')')
        .ok_or_else(|| format!("Missing closing parenthesis in: {trimmed}"))?;

    if paren_end <= paren_start {
        return Err(format!("Invalid format: {trimmed}"));
    }

    let relation = trimmed[..paren_start].trim().to_string();
    let values_str = &trimmed[paren_start + 1..paren_end];

    let mut values = Vec::new();
    for part in split_respecting_brackets(values_str) {
        let v = part.trim();
        if v.is_empty() {
            continue;
        }
        let value = parse_literal_value(v)?;
        values.push(value);
    }

    Ok((relation, crate::value::Tuple::new(values)))
}

/// Split a string by commas outside brackets and string literals.
fn split_respecting_brackets(s: &str) -> Vec<&str> {
    crate::parser::lexer::split_top_level(s, ',', crate::parser::lexer::Angles::Ignore)
}

/// Parse a literal value string into a Value.
fn parse_literal_value(s: &str) -> Result<crate::value::Value, String> {
    use crate::value::Value;
    use std::sync::Arc;

    let s = s.trim();

    if crate::parser::lexer::is_string_literal(s) {
        let inner = crate::parser::lexer::unescape(&s[1..s.len() - 1]);
        return Ok(Value::String(Arc::from(inner.as_str())));
    }

    // Boolean
    if s == "true" {
        return Ok(Value::Bool(true));
    }
    if s == "false" {
        return Ok(Value::Bool(false));
    }

    // Null
    if s.eq_ignore_ascii_case("null") {
        return Ok(Value::Null);
    }

    // Vector literal: [1.0, 2.0, 3.0]
    if s.starts_with('[') && s.ends_with(']') {
        let inner = &s[1..s.len() - 1];
        let mut vals = Vec::new();
        for part in inner.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let f: f32 = part
                .parse()
                .map_err(|_| format!("Invalid vector element: {part}"))?;
            vals.push(f);
        }
        return Ok(Value::Vector(Arc::new(vals)));
    }

    // Float (contains '.' or scientific notation 'e'/'E')
    if s.contains('.') || s.contains('e') || s.contains('E') {
        if let Ok(f) = s.parse::<f64>() {
            return Ok(Value::Float64(f));
        }
    }

    // Integer
    if let Ok(n) = s.parse::<i64>() {
        if i32::try_from(n).is_ok() {
            return Ok(Value::Int32(n as i32));
        }
        return Ok(Value::Int64(n));
    }

    Err(format!("Cannot parse value: {s}"))
}

/// Extract column schema from a query string + result tuples.
///
/// Parses variable names from the query head (e.g., `__query__(X, Y) <- ...`
/// gives columns X, Y). Falls back to col0, col1 if parsing fails.
fn extract_query_schema(query: &str, tuples: &[crate::value::Tuple]) -> Vec<ColumnDef> {
    if tuples.is_empty() {
        return vec![];
    }
    let var_names = extract_head_variables(query);
    let first = &tuples[0];
    (0..first.arity())
        .map(|i| {
            let name = var_names
                .as_ref()
                .and_then(|v| v.get(i).cloned())
                .unwrap_or_else(|| format!("col{i}"));
            let dt = first.get(i).map_or(WireDataType::String, |v| {
                WireValue::from_value(v).data_type()
            });
            ColumnDef::new(name, dt)
        })
        .collect()
}

/// Extract variable names from a query's head atom.
fn extract_head_variables(query: &str) -> Option<Vec<String>> {
    let trimmed = query.trim();
    let head = if let Some(arrow) = trimmed.find("<-") {
        &trimmed[..arrow]
    } else {
        trimmed.strip_prefix('?').unwrap_or(trimmed)
    };
    let open = head.find('(')?;
    let close = head.rfind(')')?;
    if close <= open {
        return None;
    }
    let vars: Vec<String> = head[open + 1..close]
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if vars.is_empty() {
        None
    } else {
        Some(vars)
    }
}

#[cfg(test)]
mod parsing_tests {
    use super::*;

    #[test]
    fn test_split_respecting_brackets_basic() {
        let parts = split_respecting_brackets("1, 2, 3");
        assert_eq!(parts, vec!["1", " 2", " 3"]);
    }

    #[test]
    fn test_split_respecting_brackets_quoted_string() {
        let parts = split_respecting_brackets(r#""hello, world", 1"#);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], r#""hello, world""#);
    }

    #[test]
    fn test_split_respecting_brackets_escaped_quote() {
        let parts = split_respecting_brackets(r#""value\"with\"quotes", 1"#);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], r#""value\"with\"quotes""#);
    }

    #[test]
    fn test_split_respecting_brackets_escaped_backslash_before_quote() {
        // \\" = escaped backslash + real quote (end of string)
        let parts = split_respecting_brackets(r#""path\\", 1"#);
        assert_eq!(parts.len(), 2, "got: {parts:?}");
        assert_eq!(parts[0], r#""path\\""#);
        assert_eq!(parts[1].trim(), "1");
    }

    #[test]
    fn test_split_respecting_brackets_vector() {
        let parts = split_respecting_brackets("[1.0, 2.0, 3.0], 42");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "[1.0, 2.0, 3.0]");
    }

    #[test]
    fn test_parse_literal_value_vector() {
        let val = parse_literal_value("[1.0, 2.0, 3.0]").unwrap();
        match val {
            crate::value::Value::Vector(v) => {
                assert_eq!(v.as_ref(), &[1.0f32, 2.0, 3.0]);
            }
            other => panic!("Expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_literal_value_empty_vector() {
        let val = parse_literal_value("[]").unwrap();
        match val {
            crate::value::Value::Vector(v) => {
                assert!(v.is_empty());
            }
            other => panic!("Expected empty Vector, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_literal_value_unescapes_control_chars() {
        let val = parse_literal_value(r#""a\nb\t\"c\"""#).unwrap();
        assert!(matches!(val, crate::value::Value::String(ref s) if &**s == "a\nb\t\"c\""));
    }

    #[test]
    fn test_parse_literal_value_scientific_notation() {
        let val = parse_literal_value("1e10").unwrap();
        assert!(matches!(val, crate::value::Value::Float64(f) if f == 1e10));

        let val = parse_literal_value("1.5E-3").unwrap();
        assert!(matches!(val, crate::value::Value::Float64(f) if (f - 1.5e-3).abs() < 1e-15));
    }

    #[test]
    fn test_parse_why_not_target_with_escaped_quotes() {
        let (rel, tuple) = parse_why_not_target(r#"relation("value\"with\"quotes", 1)"#).unwrap();
        assert_eq!(rel, "relation");
        assert_eq!(tuple.arity(), 2);
        // Verify the string was properly unescaped
        assert_eq!(
            tuple.get(0).unwrap(),
            &crate::value::Value::String(std::sync::Arc::from("value\"with\"quotes"))
        );
    }

    #[test]
    fn test_parse_literal_value_unescapes_strings() {
        use std::sync::Arc;
        // Simple string
        let val = parse_literal_value(r#""hello""#).unwrap();
        assert_eq!(val, crate::value::Value::String(Arc::from("hello")));

        // Escaped quote
        let val = parse_literal_value(r#""say \"hi\"""#).unwrap();
        assert_eq!(val, crate::value::Value::String(Arc::from(r#"say "hi""#)));

        // Escaped backslash
        let val = parse_literal_value(r#""path\\to""#).unwrap();
        assert_eq!(val, crate::value::Value::String(Arc::from("path\\to")));

        // Escaped backslash before closing quote: "path\\"
        let val = parse_literal_value(r#""path\\""#).unwrap();
        assert_eq!(val, crate::value::Value::String(Arc::from("path\\")));
    }

    #[test]
    fn test_split_and_parse_escaped_backslash_before_quote() {
        // Full round-trip: "path\\", 1 -> split -> parse each part
        let parts = split_respecting_brackets(r#""path\\", 1"#);
        assert_eq!(parts.len(), 2);
        let val = parse_literal_value(parts[0].trim()).unwrap();
        assert_eq!(
            val,
            crate::value::Value::String(std::sync::Arc::from("path\\"))
        );
    }

    #[test]
    fn test_extract_query_relation_query_shorthand() {
        assert_eq!(
            extract_query_relation("?edge(X, Y)"),
            Some("edge".to_string())
        );
    }

    #[test]
    fn test_extract_query_relation_internal_name() {
        let result = extract_query_relation("__query__(X) <- edge(X, _)");
        assert_eq!(result, Some("edge".to_string()));
    }

    #[test]
    fn test_extract_query_relation_other_internal_names() {
        let result = extract_query_relation("__cond_del_query__(X) <- edge(X, _)");
        assert_eq!(result, Some("edge".to_string()));
    }

    #[test]
    fn test_extract_query_relation_user_double_underscore() {
        // User-defined relation with double underscores but not matching __*__ pattern
        let result = extract_query_relation("__my_rel(X) <- base(X)");
        assert_eq!(result, Some("__my_rel".to_string()));
    }
}
