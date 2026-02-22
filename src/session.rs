//! Session Manager for Ephemeral Triggers Persistent
//!
//! Provides session-scoped ephemeral state that triggers recomputation of
//! persistent rules. Each session maintains its own ephemeral facts and rules,
//! invisible to other sessions. When queried, ephemeral + persistent data are
//! combined for execution.
//!
//! ## Architecture
//!
//! ```text
//! SessionManager
//! ├── Sessions (HashMap<SessionId, Session>)
//! │   └── Session
//! │       ├── Ephemeral facts: HashMap<relation, Vec<Tuple>>
//! │       ├── Ephemeral rules: Vec<Rule>
//! │       ├── Knowledge graph binding
//! │       └── Created/accessed timestamps
//! └── Config (max sessions, idle timeout)
//! ```
//!
//! ## Session Lifecycle
//!
//! 1. `create_session()` → SessionId
//! 2. `insert_ephemeral()` / `retract_ephemeral()` / `add_ephemeral_rule()`
//! 3. `execute_query()` → combines persistent + ephemeral
//! 4. `close_session()` → cleanup
//!
//! ## Query Execution
//!
//! - **Clean session** (no ephemeral state): Fast path → global snapshot
//! - **Dirty session** (has ephemeral state): Isolated execution with combined data
//!
//! ## Mixing Indicators
//!
//! When ephemeral facts participate in query results, the system provides:
//! - Inline warnings in response messages
//! - `ephemeral_sources` metadata in query results
//! - Per-tuple provenance tags (persistent / ephemeral / mixed)

use crate::ast::Rule;
use crate::value::Tuple;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Unique session identifier (cryptographic UUID to prevent enumeration)
pub type SessionId = String;

/// Session manager configuration
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Maximum number of concurrent sessions (0 = unlimited)
    pub max_sessions: usize,
    /// Idle timeout in seconds before session is reaped (0 = no timeout)
    pub idle_timeout_secs: u64,
    /// Maximum ephemeral facts per session (0 = unlimited)
    pub max_ephemeral_facts: usize,
    /// Maximum ephemeral rules per session (0 = unlimited)
    pub max_ephemeral_rules: usize,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_sessions: 10_000,
            idle_timeout_secs: 3600, // 1 hour
            max_ephemeral_facts: 100_000,
            max_ephemeral_rules: 1_000,
        }
    }
}

/// Provenance tag for query result tuples.
///
/// Assigned by comparing session query results against a persistent-only baseline:
/// - `Persistent`: tuple exists in both session and baseline results
/// - `Ephemeral`: tuple exists only in the session result (derived from ephemeral data)
/// - `Mixed`: reserved for future use (e.g., semiring-based provenance)
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provenance {
    /// Tuple derived entirely from persistent data
    Persistent,
    /// Tuple derived from at least one ephemeral fact/rule
    Ephemeral,
    /// Tuple derived from both persistent and ephemeral sources
    Mixed,
}

impl std::fmt::Display for Provenance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Provenance::Persistent => write!(f, "persistent"),
            Provenance::Ephemeral => write!(f, "ephemeral"),
            Provenance::Mixed => write!(f, "mixed"),
        }
    }
}

/// Metadata about ephemeral participation in a query result
#[derive(Debug, Clone, Default)]
pub struct QueryMetadata {
    /// Relations that contributed ephemeral data
    pub ephemeral_sources: Vec<String>,
    /// Warnings about ephemeral/persistent mixing
    pub warnings: Vec<String>,
    /// Whether any ephemeral data participated in the result
    pub has_ephemeral: bool,
}

/// A single session's ephemeral state
#[derive(Debug)]
pub struct Session {
    /// Unique session ID
    pub id: SessionId,
    /// Knowledge graph this session is bound to
    pub knowledge_graph: String,
    /// Ephemeral facts (relation → tuples to add)
    ephemeral_facts: HashMap<String, Vec<Tuple>>,
    /// Ephemeral rules (session-scoped, not persisted)
    ephemeral_rules: Vec<Rule>,
    /// Rule text for each ephemeral rule (for prepending to queries)
    ephemeral_rule_texts: Vec<String>,
    /// When the session was created
    pub created_at: Instant,
    /// When the session was last accessed
    pub last_accessed: Instant,
}

impl Session {
    fn new(id: SessionId, knowledge_graph: String) -> Self {
        let now = Instant::now();
        Self {
            id,
            knowledge_graph,
            ephemeral_facts: HashMap::new(),
            ephemeral_rules: Vec::new(),
            ephemeral_rule_texts: Vec::new(),
            created_at: now,
            last_accessed: now,
        }
    }

    /// Touch the session to update last-accessed time
    fn touch(&mut self) {
        self.last_accessed = Instant::now();
    }

    /// Check if session has any ephemeral state
    pub fn is_clean(&self) -> bool {
        self.ephemeral_facts.is_empty() && self.ephemeral_rules.is_empty()
    }

    /// Get the number of ephemeral facts across all relations
    pub fn ephemeral_fact_count(&self) -> usize {
        self.ephemeral_facts.values().map(Vec::len).sum()
    }

    /// Get the number of ephemeral rules
    pub fn ephemeral_rule_count(&self) -> usize {
        self.ephemeral_rules.len()
    }

    /// Insert ephemeral fact(s) into a relation
    ///
    /// These facts are only visible within this session.
    /// Duplicates with existing ephemeral facts are deduplicated.
    /// Returns the number of facts actually inserted (after dedup).
    pub fn insert_ephemeral(&mut self, relation: &str, tuples: Vec<Tuple>) -> usize {
        self.touch();
        let existing = self
            .ephemeral_facts
            .entry(relation.to_string())
            .or_default();

        let mut inserted = 0;
        for tuple in tuples {
            if !existing.contains(&tuple) {
                existing.push(tuple);
                inserted += 1;
            }
        }
        inserted
    }

    /// Retract ephemeral fact(s) from a relation
    ///
    /// Only retracts ephemeral facts. Cannot shadow/retract persistent facts.
    /// Returns the number of facts actually retracted.
    pub fn retract_ephemeral(&mut self, relation: &str, tuples: Vec<Tuple>) -> usize {
        self.touch();
        let mut retracted = 0;

        if let Some(existing) = self.ephemeral_facts.get_mut(relation) {
            for tuple in &tuples {
                if let Some(pos) = existing.iter().position(|t| t == tuple) {
                    existing.remove(pos);
                    retracted += 1;
                }
            }
            if existing.is_empty() {
                self.ephemeral_facts.remove(relation);
            }
        }

        retracted
    }

    /// Add an ephemeral rule
    ///
    /// If a persistent rule with the same head relation exists, the ephemeral
    /// version takes precedence (union semantics). A warning is generated
    /// and can be retrieved via `build_query_metadata()`.
    pub fn add_ephemeral_rule(&mut self, rule: Rule, rule_text: String) {
        self.touch();
        self.ephemeral_rules.push(rule);
        self.ephemeral_rule_texts.push(rule_text);
    }

    /// Check for rule overshadow: ephemeral rules with the same head relation
    /// as persistent rules. Returns list of overshadowed relation names.
    pub fn detect_overshadowed_rules(&self, persistent_rule_heads: &[String]) -> Vec<String> {
        let ephemeral_heads: std::collections::HashSet<&str> = self
            .ephemeral_rules
            .iter()
            .map(|r| r.head.relation.as_str())
            .collect();

        persistent_rule_heads
            .iter()
            .filter(|h| ephemeral_heads.contains(h.as_str()))
            .cloned()
            .collect()
    }

    /// Get all ephemeral facts as session_facts pairs for snapshot execution.
    ///
    /// Results are sorted by relation name for deterministic ordering,
    /// since HashMap iteration order is non-deterministic.
    pub fn session_facts(&self) -> Vec<(String, Tuple)> {
        let mut facts = Vec::new();
        let mut relations: Vec<&String> = self.ephemeral_facts.keys().collect();
        relations.sort();
        for relation in relations {
            if let Some(tuples) = self.ephemeral_facts.get(relation) {
                for tuple in tuples {
                    facts.push((relation.clone(), tuple.clone()));
                }
            }
        }
        facts
    }

    /// Get ephemeral rule texts for prepending to queries
    pub fn rule_texts(&self) -> &[String] {
        &self.ephemeral_rule_texts
    }

    /// Get ephemeral rules (AST)
    pub fn rules(&self) -> &[Rule] {
        &self.ephemeral_rules
    }

    /// Get all ephemeral facts
    pub fn ephemeral_facts(&self) -> &HashMap<String, Vec<Tuple>> {
        &self.ephemeral_facts
    }

    /// Clear all ephemeral state (e.g., on KG switch)
    pub fn clear(&mut self) {
        self.ephemeral_facts.clear();
        self.ephemeral_rules.clear();
        self.ephemeral_rule_texts.clear();
    }

    /// Remove an ephemeral rule by index
    pub fn remove_ephemeral_rule(&mut self, index: usize) {
        if index < self.ephemeral_rules.len() {
            self.ephemeral_rules.remove(index);
            self.ephemeral_rule_texts.remove(index);
        }
    }

    /// Remove all ephemeral rules for a given relation name
    pub fn remove_ephemeral_rules_by_name(&mut self, name: &str) {
        let mut i = 0;
        while i < self.ephemeral_rules.len() {
            if self.ephemeral_rules[i].head.relation == name {
                self.ephemeral_rules.remove(i);
                self.ephemeral_rule_texts.remove(i);
            } else {
                i += 1;
            }
        }
    }

    /// Build query metadata describing ephemeral participation.
    ///
    /// Optionally accepts persistent rule head names to detect overshadowing.
    pub fn build_query_metadata(&self) -> QueryMetadata {
        self.build_query_metadata_with_persistent(&[])
    }

    /// Build query metadata with persistent rule overshadow detection.
    pub fn build_query_metadata_with_persistent(
        &self,
        persistent_rule_heads: &[String],
    ) -> QueryMetadata {
        if self.is_clean() {
            return QueryMetadata::default();
        }

        let mut metadata = QueryMetadata {
            has_ephemeral: true,
            ..Default::default()
        };

        // List relations with ephemeral data (sorted for deterministic output)
        let mut sources: Vec<String> = self.ephemeral_facts.keys().cloned().collect();
        sources.sort();
        metadata.ephemeral_sources = sources;

        // Add info messages
        if !self.ephemeral_facts.is_empty() {
            let n = self.ephemeral_facts.len();
            metadata.warnings.push(format!(
                "Results include session data from {} {}: {}",
                n,
                if n == 1 { "relation" } else { "relations" },
                metadata.ephemeral_sources.join(", ")
            ));
        }

        if !self.ephemeral_rules.is_empty() {
            let n = self.ephemeral_rules.len();
            metadata.warnings.push(format!(
                "Results use {} session {}",
                n,
                if n == 1 { "rule" } else { "rules" }
            ));
        }

        // Detect rule overshadowing
        let overshadowed = self.detect_overshadowed_rules(persistent_rule_heads);
        if !overshadowed.is_empty() {
            metadata.warnings.push(format!(
                "Session rules override persistent rules for: {}",
                overshadowed.join(", ")
            ));
        }

        metadata
    }
}

/// Manages all active sessions
///
/// Thread-safe via internal RwLock. Sessions are identified by cryptographic
/// UUIDs to prevent enumeration attacks. All operations are recorded in the audit log.
pub struct SessionManager {
    sessions: RwLock<HashMap<SessionId, Session>>,
    config: SessionConfig,
    audit: AuditLog,
}

impl SessionManager {
    /// Create a new session manager with the given configuration
    pub fn new(config: SessionConfig) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            config,
            audit: AuditLog::default(),
        }
    }

    /// Create a new session bound to a knowledge graph
    ///
    /// Returns the session ID (UUID), or an error if max sessions exceeded.
    pub fn create_session(&self, knowledge_graph: &str) -> Result<SessionId, String> {
        let mut sessions = self.sessions.write();

        // Check max sessions limit
        if self.config.max_sessions > 0 && sessions.len() >= self.config.max_sessions {
            return Err(format!(
                "Maximum number of sessions ({}) exceeded",
                self.config.max_sessions
            ));
        }

        let id = uuid::Uuid::new_v4().to_string();
        let session = Session::new(id.clone(), knowledge_graph.to_string());
        sessions.insert(id.clone(), session);

        self.audit.record(AuditEvent::SessionCreated {
            session_id: id.clone(),
            knowledge_graph: knowledge_graph.to_string(),
            timestamp: Instant::now(),
        });

        Ok(id)
    }

    /// Close and clean up a session
    pub fn close_session(&self, id: &SessionId) -> Result<(), String> {
        let mut sessions = self.sessions.write();
        sessions
            .remove(id)
            .ok_or_else(|| format!("Session {id} not found"))?;

        self.audit.record(AuditEvent::SessionClosed {
            session_id: id.clone(),
            timestamp: Instant::now(),
        });

        Ok(())
    }

    /// Check if a session exists
    pub fn has_session(&self, id: &SessionId) -> bool {
        self.sessions.read().contains_key(id)
    }

    /// Get the number of active sessions
    pub fn session_count(&self) -> usize {
        self.sessions.read().len()
    }

    /// Execute a function with read access to a session
    pub fn with_session<F, R>(&self, id: &SessionId, f: F) -> Result<R, String>
    where
        F: FnOnce(&Session) -> R,
    {
        let sessions = self.sessions.read();
        let session = sessions
            .get(id)
            .ok_or_else(|| format!("Session {id} not found"))?;
        Ok(f(session))
    }

    /// Execute a function with write access to a session
    pub fn with_session_mut<F, R>(&self, id: &SessionId, f: F) -> Result<R, String>
    where
        F: FnOnce(&mut Session) -> R,
    {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(id)
            .ok_or_else(|| format!("Session {id} not found"))?;
        Ok(f(session))
    }

    /// Update last-accessed timestamp for a session to prevent idle reaping.
    ///
    /// Call this for operations that represent genuine session activity
    /// but only require read access (e.g., query execution).
    pub fn touch_session(&self, id: &SessionId) -> Result<(), String> {
        self.with_session_mut(id, Session::touch)
    }

    /// Insert ephemeral facts into a session.
    /// Returns the number of facts actually inserted (after dedup).
    pub fn insert_ephemeral(
        &self,
        session_id: &SessionId,
        relation: &str,
        tuples: Vec<Tuple>,
    ) -> Result<usize, String> {
        if relation.trim().is_empty() {
            return Err("Relation name cannot be empty".to_string());
        }
        let inserted = self.with_session_mut(session_id, |session| {
            // Enforce per-session fact limit
            if self.config.max_ephemeral_facts > 0 {
                let current = session.ephemeral_fact_count();
                let incoming = tuples.len();
                if current + incoming > self.config.max_ephemeral_facts {
                    return Err(format!(
                        "Session ephemeral fact limit exceeded: {} + {} > {} max",
                        current, incoming, self.config.max_ephemeral_facts
                    ));
                }
            }
            session.touch();
            Ok(session.insert_ephemeral(relation, tuples))
        })??;

        self.audit.record(AuditEvent::EphemeralInsert {
            session_id: session_id.clone(),
            relation: relation.to_string(),
            count: inserted,
            timestamp: Instant::now(),
        });

        Ok(inserted)
    }

    /// Retract ephemeral facts from a session
    pub fn retract_ephemeral(
        &self,
        session_id: &SessionId,
        relation: &str,
        tuples: Vec<Tuple>,
    ) -> Result<usize, String> {
        if relation.trim().is_empty() {
            return Err("Relation name cannot be empty".to_string());
        }
        let retracted = self.with_session_mut(session_id, |session| {
            session.touch();
            session.retract_ephemeral(relation, tuples)
        })?;

        if retracted > 0 {
            self.audit.record(AuditEvent::EphemeralRetract {
                session_id: session_id.clone(),
                relation: relation.to_string(),
                count: retracted,
                timestamp: Instant::now(),
            });
        }

        Ok(retracted)
    }

    /// Add an ephemeral rule to a session
    pub fn add_ephemeral_rule(
        &self,
        session_id: &SessionId,
        rule: Rule,
        rule_text: String,
    ) -> Result<(), String> {
        let head_relation = rule.head.relation.clone();
        self.with_session_mut(session_id, |session| {
            // Enforce per-session rule limit
            if self.config.max_ephemeral_rules > 0
                && session.ephemeral_rule_count() >= self.config.max_ephemeral_rules
            {
                return Err(format!(
                    "Session ephemeral rule limit exceeded: {} >= {} max",
                    session.ephemeral_rule_count(),
                    self.config.max_ephemeral_rules
                ));
            }
            session.touch();
            session.add_ephemeral_rule(rule, rule_text);
            Ok(())
        })??;

        self.audit.record(AuditEvent::EphemeralRuleAdded {
            session_id: session_id.clone(),
            head_relation,
            timestamp: Instant::now(),
        });

        Ok(())
    }

    /// Get session facts for snapshot execution
    pub fn get_session_facts(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<(String, Tuple)>, String> {
        self.with_session(session_id, Session::session_facts)
    }

    /// Check if a session is clean (no ephemeral state)
    pub fn is_session_clean(&self, session_id: &SessionId) -> Result<bool, String> {
        self.with_session(session_id, Session::is_clean)
    }

    /// Get the knowledge graph a session is bound to
    pub fn session_kg(&self, session_id: &SessionId) -> Result<String, String> {
        self.with_session(session_id, |session| session.knowledge_graph.clone())
    }

    /// Get query metadata for a session
    pub fn get_query_metadata(&self, session_id: &SessionId) -> Result<QueryMetadata, String> {
        self.with_session(session_id, Session::build_query_metadata)
    }

    /// Clear all ephemeral state for a session (e.g., on KG switch)
    pub fn clear_session(&self, session_id: &SessionId) -> Result<(), String> {
        self.with_session_mut(session_id, Session::clear)?;

        self.audit.record(AuditEvent::SessionCleared {
            session_id: session_id.clone(),
            timestamp: Instant::now(),
        });

        Ok(())
    }

    /// Switch a session to a different knowledge graph
    ///
    /// Clears all ephemeral state per the design decision.
    /// Reads old KG and writes new KG atomically under a single write lock.
    pub fn switch_kg(&self, session_id: &SessionId, new_kg: &str) -> Result<(), String> {
        let from_kg = self.with_session_mut(session_id, |session| {
            let from = session.knowledge_graph.clone();
            session.clear();
            session.knowledge_graph = new_kg.to_string();
            from
        })?;

        self.audit.record(AuditEvent::KgSwitched {
            session_id: session_id.clone(),
            from_kg,
            to_kg: new_kg.to_string(),
            timestamp: Instant::now(),
        });

        Ok(())
    }

    /// Reap expired sessions based on idle timeout
    ///
    /// Returns the number of sessions reaped.
    /// Close all sessions bound to a specific knowledge graph.
    /// Called when a KG is dropped to prevent stale sessions from lingering.
    pub fn close_sessions_for_kg(&self, kg: &str) -> usize {
        let mut sessions = self.sessions.write();
        let before = sessions.len();
        sessions.retain(|_, session| session.knowledge_graph != kg);
        let closed = before - sessions.len();
        if closed > 0 {
            drop(sessions);
            tracing::info!(kg, closed, "sessions_closed_for_dropped_kg");
        }
        closed
    }

    pub fn reap_expired(&self) -> usize {
        if self.config.idle_timeout_secs == 0 {
            return 0;
        }

        let timeout = std::time::Duration::from_secs(self.config.idle_timeout_secs);
        let now = Instant::now();
        let mut sessions = self.sessions.write();
        let before = sessions.len();
        sessions.retain(|_, session| now.duration_since(session.last_accessed) < timeout);
        let reaped = before - sessions.len();

        if reaped > 0 {
            // Drop the write lock before recording audit event
            drop(sessions);
            self.audit.record(AuditEvent::SessionsReaped {
                count: reaped,
                timestamp: Instant::now(),
            });
        }

        reaped
    }

    /// Get summary statistics
    pub fn stats(&self) -> SessionStats {
        let sessions = self.sessions.read();
        let total = sessions.len();
        let clean = sessions.values().filter(|s| s.is_clean()).count();
        let dirty = total - clean;
        let total_ephemeral_facts: usize =
            sessions.values().map(Session::ephemeral_fact_count).sum();
        let total_ephemeral_rules: usize =
            sessions.values().map(Session::ephemeral_rule_count).sum();

        SessionStats {
            total_sessions: total,
            clean_sessions: clean,
            dirty_sessions: dirty,
            total_ephemeral_facts,
            total_ephemeral_rules,
        }
    }

    /// List all session IDs (sorted for deterministic output)
    pub fn list_sessions(&self) -> Vec<SessionId> {
        let mut ids: Vec<SessionId> = self.sessions.read().keys().cloned().collect();
        ids.sort_unstable();
        ids
    }

    /// Get a reference to the audit log
    pub fn audit_log(&self) -> &AuditLog {
        &self.audit
    }

    /// Record a query-with-ephemeral audit event
    pub fn record_query_with_ephemeral(
        &self,
        session_id: &SessionId,
        ephemeral_sources: Vec<String>,
        result_count: usize,
        execution_time_ms: u64,
    ) {
        self.audit.record(AuditEvent::QueryWithEphemeral {
            session_id: session_id.clone(),
            ephemeral_sources,
            result_count,
            execution_time_ms,
            timestamp: Instant::now(),
        });
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new(SessionConfig::default())
    }
}

/// Summary statistics about sessions
#[derive(Debug, Clone)]
pub struct SessionStats {
    pub total_sessions: usize,
    pub clean_sessions: usize,
    pub dirty_sessions: usize,
    pub total_ephemeral_facts: usize,
    pub total_ephemeral_rules: usize,
}

// === Audit Event System ===

/// Structured audit event for session operations
///
/// Provides a structured event stream for tracking session lifecycle,
/// ephemeral data mutations, and query execution.
#[derive(Debug, Clone)]
pub enum AuditEvent {
    /// Session was created
    SessionCreated {
        session_id: SessionId,
        knowledge_graph: String,
        timestamp: Instant,
    },
    /// Session was closed
    SessionClosed {
        session_id: SessionId,
        timestamp: Instant,
    },
    /// Ephemeral facts were inserted
    EphemeralInsert {
        session_id: SessionId,
        relation: String,
        count: usize,
        timestamp: Instant,
    },
    /// Ephemeral facts were retracted
    EphemeralRetract {
        session_id: SessionId,
        relation: String,
        count: usize,
        timestamp: Instant,
    },
    /// Ephemeral rule was added
    EphemeralRuleAdded {
        session_id: SessionId,
        head_relation: String,
        timestamp: Instant,
    },
    /// Query executed with ephemeral participation
    QueryWithEphemeral {
        session_id: SessionId,
        ephemeral_sources: Vec<String>,
        result_count: usize,
        execution_time_ms: u64,
        timestamp: Instant,
    },
    /// Session ephemeral state was cleared
    SessionCleared {
        session_id: SessionId,
        timestamp: Instant,
    },
    /// Session KG was switched
    KgSwitched {
        session_id: SessionId,
        from_kg: String,
        to_kg: String,
        timestamp: Instant,
    },
    /// Sessions were reaped due to idle timeout
    SessionsReaped { count: usize, timestamp: Instant },
}

/// Audit log that records session events
///
/// Thread-safe event buffer with configurable capacity.
/// When the buffer is full, oldest events are discarded.
/// Uses a `drain_offset` to track how many events have been drained,
/// allowing `events_since` to work correctly across drains.
pub struct AuditLog {
    events: RwLock<Vec<AuditEvent>>,
    max_events: usize,
    /// Tracks the total number of events drained over the log's lifetime.
    /// This offset is added to the current Vec index to get the "logical" index.
    drain_offset: AtomicU64,
}

impl AuditLog {
    /// Create a new audit log with the given capacity
    pub fn new(max_events: usize) -> Self {
        Self {
            events: RwLock::new(Vec::with_capacity(max_events.min(10_000))),
            max_events,
            drain_offset: AtomicU64::new(0),
        }
    }

    /// Record an audit event
    pub fn record(&self, event: AuditEvent) {
        // max_events == 0 means unlimited logging
        if self.max_events == 0 {
            self.events.write().push(event);
            return;
        }
        let mut events = self.events.write();
        if events.len() >= self.max_events {
            // Remove oldest half when full (at least 1 to guarantee progress)
            let drain_count = (self.max_events / 2).max(1);
            events.drain(..drain_count);
            self.drain_offset
                .fetch_add(drain_count as u64, Ordering::SeqCst);
        }
        events.push(event);
    }

    /// Get all recorded events
    pub fn events(&self) -> Vec<AuditEvent> {
        self.events.read().clone()
    }

    /// Get events since a given logical index.
    ///
    /// The logical index accounts for drained events, so it remains valid
    /// even after the buffer wraps. Use `logical_len()` to get the current
    /// logical index for later calls to `events_since()`.
    pub fn events_since(&self, logical_start: usize) -> Vec<AuditEvent> {
        let events = self.events.read();
        let offset = self.drain_offset.load(Ordering::SeqCst) as usize;
        // Convert logical index to physical index
        let physical_start = logical_start.saturating_sub(offset);
        if physical_start >= events.len() {
            Vec::new()
        } else {
            events[physical_start..].to_vec()
        }
    }

    /// Get the number of currently buffered events
    pub fn len(&self) -> usize {
        self.events.read().len()
    }

    /// Get the logical length (total events ever recorded minus those cleared).
    /// Use this value for subsequent `events_since()` calls.
    pub fn logical_len(&self) -> usize {
        let events = self.events.read();
        let offset = self.drain_offset.load(Ordering::SeqCst) as usize;
        events.len() + offset
    }

    /// Check if the audit log is empty
    pub fn is_empty(&self) -> bool {
        self.events.read().is_empty()
    }

    /// Clear all events and reset logical offset.
    /// Both operations are performed while holding the write lock for atomicity.
    pub fn clear(&self) {
        let mut events = self.events.write();
        events.clear();
        self.drain_offset.store(0, Ordering::SeqCst);
        // Write lock is held until here, ensuring no concurrent record()
        // sees a stale drain_offset with an empty events Vec.
    }
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new(10_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;
    use std::sync::Arc;

    fn make_tuple(vals: Vec<i64>) -> Tuple {
        Tuple::new(vals.into_iter().map(Value::Int64).collect())
    }

    // === Session Lifecycle ===

    #[test]
    fn test_create_session() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();
        assert!(!id.is_empty());
        assert!(mgr.has_session(&id));
        assert_eq!(mgr.session_count(), 1);
    }

    #[test]
    fn test_create_multiple_sessions() {
        let mgr = SessionManager::default();
        let id1 = mgr.create_session("kg1").unwrap();
        let id2 = mgr.create_session("kg2").unwrap();
        let id3 = mgr.create_session("kg1").unwrap();

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_eq!(mgr.session_count(), 3);
    }

    #[test]
    fn test_close_session() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();
        assert!(mgr.has_session(&id));

        mgr.close_session(&id).unwrap();
        assert!(!mgr.has_session(&id));
        assert_eq!(mgr.session_count(), 0);
    }

    #[test]
    fn test_close_nonexistent_session() {
        let mgr = SessionManager::default();
        assert!(mgr.close_session(&"nonexistent".to_string()).is_err());
    }

    #[test]
    fn test_max_sessions_limit() {
        let config = SessionConfig {
            max_sessions: 2,
            idle_timeout_secs: 0,
            ..Default::default()
        };
        let mgr = SessionManager::new(config);

        mgr.create_session("kg1").unwrap();
        mgr.create_session("kg2").unwrap();
        assert!(mgr.create_session("kg3").is_err());
    }

    // === Clean/Dirty Detection ===

    #[test]
    fn test_new_session_is_clean() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();
        assert!(mgr.is_session_clean(&id).unwrap());
    }

    #[test]
    fn test_session_dirty_after_ephemeral_insert() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        mgr.insert_ephemeral(&id, "edge", vec![make_tuple(vec![1, 2])])
            .unwrap();

        assert!(!mgr.is_session_clean(&id).unwrap());
    }

    #[test]
    fn test_session_clean_after_retract_all() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        let t = make_tuple(vec![1, 2]);
        mgr.insert_ephemeral(&id, "edge", vec![t.clone()]).unwrap();
        assert!(!mgr.is_session_clean(&id).unwrap());

        mgr.retract_ephemeral(&id, "edge", vec![t]).unwrap();
        assert!(mgr.is_session_clean(&id).unwrap());
    }

    // === Ephemeral Facts ===

    #[test]
    fn test_insert_ephemeral_facts() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        mgr.insert_ephemeral(
            &id,
            "edge",
            vec![make_tuple(vec![1, 2]), make_tuple(vec![2, 3])],
        )
        .unwrap();

        let facts = mgr.get_session_facts(&id).unwrap();
        assert_eq!(facts.len(), 2);
    }

    #[test]
    fn test_ephemeral_deduplication() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        let t = make_tuple(vec![1, 2]);
        mgr.insert_ephemeral(&id, "edge", vec![t.clone()]).unwrap();
        mgr.insert_ephemeral(&id, "edge", vec![t]).unwrap();

        let facts = mgr.get_session_facts(&id).unwrap();
        assert_eq!(facts.len(), 1);
    }

    #[test]
    fn test_retract_ephemeral_facts() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        let t1 = make_tuple(vec![1, 2]);
        let t2 = make_tuple(vec![2, 3]);
        mgr.insert_ephemeral(&id, "edge", vec![t1.clone(), t2.clone()])
            .unwrap();

        let retracted = mgr.retract_ephemeral(&id, "edge", vec![t1]).unwrap();
        assert_eq!(retracted, 1);

        let facts = mgr.get_session_facts(&id).unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].1, t2);
    }

    #[test]
    fn test_retract_nonexistent_fact() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        let retracted = mgr
            .retract_ephemeral(&id, "edge", vec![make_tuple(vec![99, 99])])
            .unwrap();
        assert_eq!(retracted, 0);
    }

    // === Ephemeral Rules ===

    #[test]
    fn test_add_ephemeral_rule() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        let rule = crate::ast::Rule {
            head: crate::ast::Atom {
                relation: "path".to_string(),
                args: vec![
                    crate::ast::Term::Variable("X".to_string()),
                    crate::ast::Term::Variable("Y".to_string()),
                ],
            },
            body: vec![crate::ast::BodyPredicate::Positive(crate::ast::Atom {
                relation: "edge".to_string(),
                args: vec![
                    crate::ast::Term::Variable("X".to_string()),
                    crate::ast::Term::Variable("Y".to_string()),
                ],
            })],
        };

        mgr.add_ephemeral_rule(&id, rule, "path(X, Y) <- edge(X, Y)".to_string())
            .unwrap();

        mgr.with_session(&id, |session| {
            assert_eq!(session.ephemeral_rule_count(), 1);
            assert_eq!(session.rule_texts().len(), 1);
        })
        .unwrap();
    }

    // === Session Facts for Execution ===

    #[test]
    fn test_session_facts_multiple_relations() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        mgr.insert_ephemeral(&id, "edge", vec![make_tuple(vec![1, 2])])
            .unwrap();
        mgr.insert_ephemeral(&id, "node", vec![make_tuple(vec![1]), make_tuple(vec![2])])
            .unwrap();

        let facts = mgr.get_session_facts(&id).unwrap();
        assert_eq!(facts.len(), 3);

        // Check that both relations are represented
        let relations: Vec<&str> = facts.iter().map(|(r, _)| r.as_str()).collect();
        assert!(relations.contains(&"edge"));
        assert!(relations.contains(&"node"));
    }

    // === KG Switch ===

    #[test]
    fn test_switch_kg_clears_ephemeral() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("kg1").unwrap();

        mgr.insert_ephemeral(&id, "edge", vec![make_tuple(vec![1, 2])])
            .unwrap();
        assert!(!mgr.is_session_clean(&id).unwrap());

        mgr.switch_kg(&id, "kg2").unwrap();
        assert!(mgr.is_session_clean(&id).unwrap());
        assert_eq!(mgr.session_kg(&id).unwrap(), "kg2");
    }

    // === Clear ===

    #[test]
    fn test_clear_session() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        mgr.insert_ephemeral(&id, "edge", vec![make_tuple(vec![1, 2])])
            .unwrap();
        mgr.add_ephemeral_rule(
            &id,
            crate::ast::Rule {
                head: crate::ast::Atom {
                    relation: "test".to_string(),
                    args: vec![],
                },
                body: vec![],
            },
            "test() <-".to_string(),
        )
        .unwrap();

        assert!(!mgr.is_session_clean(&id).unwrap());
        mgr.clear_session(&id).unwrap();
        assert!(mgr.is_session_clean(&id).unwrap());
    }

    // === Query Metadata ===

    #[test]
    fn test_clean_session_metadata() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        let metadata = mgr.get_query_metadata(&id).unwrap();
        assert!(!metadata.has_ephemeral);
        assert!(metadata.ephemeral_sources.is_empty());
        assert!(metadata.warnings.is_empty());
    }

    #[test]
    fn test_dirty_session_metadata() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        mgr.insert_ephemeral(&id, "edge", vec![make_tuple(vec![1, 2])])
            .unwrap();

        let metadata = mgr.get_query_metadata(&id).unwrap();
        assert!(metadata.has_ephemeral);
        assert!(metadata.ephemeral_sources.contains(&"edge".to_string()));
        assert!(!metadata.warnings.is_empty());
    }

    // === Statistics ===

    #[test]
    fn test_session_stats() {
        let mgr = SessionManager::default();
        let id1 = mgr.create_session("kg1").unwrap();
        let _id2 = mgr.create_session("kg2").unwrap();

        mgr.insert_ephemeral(
            &id1,
            "edge",
            vec![make_tuple(vec![1, 2]), make_tuple(vec![2, 3])],
        )
        .unwrap();

        let stats = mgr.stats();
        assert_eq!(stats.total_sessions, 2);
        assert_eq!(stats.clean_sessions, 1);
        assert_eq!(stats.dirty_sessions, 1);
        assert_eq!(stats.total_ephemeral_facts, 2);
    }

    // === Reaping ===

    #[test]
    fn test_reap_no_timeout() {
        let config = SessionConfig {
            max_sessions: 100,
            idle_timeout_secs: 0,
            ..Default::default()
        };
        let mgr = SessionManager::new(config);
        mgr.create_session("kg1").unwrap();

        assert_eq!(mgr.reap_expired(), 0);
    }

    // === Concurrent Access ===

    #[test]
    fn test_concurrent_session_creation() {
        let mgr = Arc::new(SessionManager::default());
        let mut handles = vec![];

        for i in 0..10 {
            let mgr = Arc::clone(&mgr);
            handles.push(std::thread::spawn(move || {
                mgr.create_session(&format!("kg{i}")).unwrap()
            }));
        }

        let ids: Vec<SessionId> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All IDs are unique
        let mut sorted = ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len());

        assert_eq!(mgr.session_count(), 10);
    }

    // === Re-insert after retract ===

    #[test]
    fn test_reinsert_after_retract() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        let t = make_tuple(vec![1, 2]);
        mgr.insert_ephemeral(&id, "edge", vec![t.clone()]).unwrap();
        mgr.retract_ephemeral(&id, "edge", vec![t.clone()]).unwrap();
        assert!(mgr.is_session_clean(&id).unwrap());

        mgr.insert_ephemeral(&id, "edge", vec![t]).unwrap();
        assert!(!mgr.is_session_clean(&id).unwrap());
        assert_eq!(mgr.get_session_facts(&id).unwrap().len(), 1);
    }

    // === List sessions ===

    #[test]
    fn test_list_sessions() {
        let mgr = SessionManager::default();
        let id1 = mgr.create_session("kg1").unwrap();
        let id2 = mgr.create_session("kg2").unwrap();

        let mut ids = mgr.list_sessions();
        ids.sort();
        let mut expected = vec![id1, id2];
        expected.sort();
        assert_eq!(ids, expected);
    }

    // === Rule Overshadow Detection ===

    #[test]
    fn test_detect_overshadowed_rules() {
        let mut session = Session::new("test-1".to_string(), "default".to_string());

        // Add ephemeral rule for "path"
        session.add_ephemeral_rule(
            crate::ast::Rule {
                head: crate::ast::Atom {
                    relation: "path".to_string(),
                    args: vec![
                        crate::ast::Term::Variable("X".to_string()),
                        crate::ast::Term::Variable("Y".to_string()),
                    ],
                },
                body: vec![crate::ast::BodyPredicate::Positive(crate::ast::Atom {
                    relation: "edge".to_string(),
                    args: vec![
                        crate::ast::Term::Variable("X".to_string()),
                        crate::ast::Term::Variable("Y".to_string()),
                    ],
                })],
            },
            "path(X, Y) <- edge(X, Y)".to_string(),
        );

        // Check against persistent rules
        let persistent_heads = vec!["path".to_string(), "reachable".to_string()];
        let overshadowed = session.detect_overshadowed_rules(&persistent_heads);
        assert_eq!(overshadowed, vec!["path".to_string()]);

        // No overlap
        let persistent_heads = vec!["other".to_string()];
        let overshadowed = session.detect_overshadowed_rules(&persistent_heads);
        assert!(overshadowed.is_empty());
    }

    #[test]
    fn test_metadata_with_overshadow() {
        let mut session = Session::new("test-1".to_string(), "default".to_string());

        session.add_ephemeral_rule(
            crate::ast::Rule {
                head: crate::ast::Atom {
                    relation: "path".to_string(),
                    args: vec![],
                },
                body: vec![],
            },
            "path() <-".to_string(),
        );

        let metadata = session
            .build_query_metadata_with_persistent(&["path".to_string(), "other".to_string()]);
        assert!(metadata.has_ephemeral);
        assert!(metadata.warnings.iter().any(|w| w.contains("override")));
    }

    // === Session-local relations (no schema required) ===

    #[test]
    fn test_session_local_relations() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        // Insert into a relation that doesn't exist in persistent store
        mgr.insert_ephemeral(
            &id,
            "query_embedding",
            vec![Tuple::new(vec![Value::vector(vec![1.0, 2.0, 3.0])])],
        )
        .unwrap();

        let facts = mgr.get_session_facts(&id).unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].0, "query_embedding");
    }

    // === Concurrent insert + retract ===

    #[test]
    fn test_concurrent_session_operations() {
        let mgr = Arc::new(SessionManager::default());
        let id = mgr.create_session("default").unwrap();

        let mut handles = vec![];
        for i in 0..10 {
            let mgr = Arc::clone(&mgr);
            let id = id.clone();
            handles.push(std::thread::spawn(move || {
                mgr.insert_ephemeral(&id, "data", vec![make_tuple(vec![i])])
                    .unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let facts = mgr.get_session_facts(&id).unwrap();
        assert_eq!(facts.len(), 10);
    }

    // === Multiple sessions isolation ===

    #[test]
    fn test_session_isolation() {
        let mgr = SessionManager::default();
        let s1 = mgr.create_session("default").unwrap();
        let s2 = mgr.create_session("default").unwrap();

        // Insert different facts in each session
        mgr.insert_ephemeral(&s1, "edge", vec![make_tuple(vec![1, 2])])
            .unwrap();
        mgr.insert_ephemeral(&s2, "edge", vec![make_tuple(vec![3, 4])])
            .unwrap();

        // Each session sees only its own facts
        let facts1 = mgr.get_session_facts(&s1).unwrap();
        let facts2 = mgr.get_session_facts(&s2).unwrap();

        assert_eq!(facts1.len(), 1);
        assert_eq!(facts2.len(), 1);
        assert_ne!(facts1[0].1, facts2[0].1);
    }

    // === Audit Log ===

    #[test]
    fn test_audit_session_lifecycle() {
        let mgr = SessionManager::default();
        assert!(mgr.audit_log().is_empty());

        let id = mgr.create_session("default").unwrap();
        assert_eq!(mgr.audit_log().len(), 1);

        mgr.close_session(&id).unwrap();
        assert_eq!(mgr.audit_log().len(), 2);

        let events = mgr.audit_log().events();
        assert!(matches!(events[0], AuditEvent::SessionCreated { .. }));
        assert!(matches!(events[1], AuditEvent::SessionClosed { .. }));
    }

    #[test]
    fn test_audit_ephemeral_operations() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        mgr.insert_ephemeral(&id, "edge", vec![make_tuple(vec![1, 2])])
            .unwrap();
        mgr.retract_ephemeral(&id, "edge", vec![make_tuple(vec![1, 2])])
            .unwrap();

        let events = mgr.audit_log().events();
        // SessionCreated + EphemeralInsert + EphemeralRetract
        assert_eq!(events.len(), 3);
        assert!(matches!(
            events[1],
            AuditEvent::EphemeralInsert { count: 1, .. }
        ));
        assert!(matches!(
            events[2],
            AuditEvent::EphemeralRetract { count: 1, .. }
        ));
    }

    #[test]
    fn test_audit_retract_zero_no_event() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        // Retract something that doesn't exist → 0 retracted → no audit event
        mgr.retract_ephemeral(&id, "edge", vec![make_tuple(vec![99, 99])])
            .unwrap();

        let events = mgr.audit_log().events();
        // Only SessionCreated, no EphemeralRetract
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_audit_ephemeral_rule() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        let rule = crate::ast::Rule {
            head: crate::ast::Atom {
                relation: "path".to_string(),
                args: vec![],
            },
            body: vec![],
        };

        mgr.add_ephemeral_rule(&id, rule, "path() <-".to_string())
            .unwrap();

        let events = mgr.audit_log().events();
        assert_eq!(events.len(), 2);
        match &events[1] {
            AuditEvent::EphemeralRuleAdded { head_relation, .. } => {
                assert_eq!(head_relation, "path");
            }
            other => panic!("Expected EphemeralRuleAdded, got {other:?}"),
        }
    }

    #[test]
    fn test_audit_clear_and_switch_kg() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("kg1").unwrap();

        mgr.insert_ephemeral(&id, "edge", vec![make_tuple(vec![1, 2])])
            .unwrap();
        mgr.clear_session(&id).unwrap();

        let events = mgr.audit_log().events();
        // SessionCreated + EphemeralInsert + SessionCleared
        assert_eq!(events.len(), 3);
        assert!(matches!(events[2], AuditEvent::SessionCleared { .. }));

        mgr.switch_kg(&id, "kg2").unwrap();
        let events = mgr.audit_log().events();
        // + KgSwitched
        assert_eq!(events.len(), 4);
        match &events[3] {
            AuditEvent::KgSwitched { from_kg, to_kg, .. } => {
                assert_eq!(from_kg, "kg1");
                assert_eq!(to_kg, "kg2");
            }
            other => panic!("Expected KgSwitched, got {other:?}"),
        }
    }

    #[test]
    fn test_audit_query_with_ephemeral() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        mgr.record_query_with_ephemeral(&id, vec!["edge".to_string()], 5, 42);

        let events = mgr.audit_log().events();
        assert_eq!(events.len(), 2); // SessionCreated + QueryWithEphemeral
        match &events[1] {
            AuditEvent::QueryWithEphemeral {
                ephemeral_sources,
                result_count,
                execution_time_ms,
                ..
            } => {
                assert_eq!(ephemeral_sources, &["edge".to_string()]);
                assert_eq!(*result_count, 5);
                assert_eq!(*execution_time_ms, 42);
            }
            other => panic!("Expected QueryWithEphemeral, got {other:?}"),
        }
    }

    #[test]
    fn test_audit_log_capacity() {
        let log = AuditLog::new(4);
        for i in 0..6 {
            log.record(AuditEvent::SessionCreated {
                session_id: i.to_string(),
                knowledge_graph: "test".to_string(),
                timestamp: Instant::now(),
            });
        }
        // Max 4 events, but when full (4), oldest half (2) are drained,
        // then events 5 and 6 are added → should have 4 events
        assert!(log.len() <= 4);
    }

    #[test]
    fn test_audit_events_since() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();
        mgr.insert_ephemeral(&id, "a", vec![make_tuple(vec![1])])
            .unwrap();
        mgr.insert_ephemeral(&id, "b", vec![make_tuple(vec![2])])
            .unwrap();

        // events_since(2) should return events after index 2
        let since = mgr.audit_log().events_since(2);
        assert_eq!(since.len(), 1); // only the last insert
        assert!(matches!(since[0], AuditEvent::EphemeralInsert { .. }));
    }

    #[test]
    fn test_audit_events_since_after_drain() {
        let log = AuditLog::new(4);

        // Record 4 events (fills buffer)
        for i in 0..4 {
            log.record(AuditEvent::SessionCreated {
                session_id: i.to_string(),
                knowledge_graph: "test".to_string(),
                timestamp: Instant::now(),
            });
        }

        // Save the logical position after 4 events
        let checkpoint = log.logical_len();
        assert_eq!(checkpoint, 4);

        // Record 2 more events (triggers drain of oldest 2)
        for i in 4..6 {
            log.record(AuditEvent::SessionCreated {
                session_id: i.to_string(),
                knowledge_graph: "test".to_string(),
                timestamp: Instant::now(),
            });
        }

        // events_since(checkpoint) should return only the 2 new events
        let since = log.events_since(checkpoint);
        assert_eq!(since.len(), 2);
        match &since[0] {
            AuditEvent::SessionCreated { session_id, .. } => assert_eq!(session_id, "4"),
            other => panic!("Expected SessionCreated, got {other:?}"),
        }
    }

    #[test]
    fn test_audit_logical_len() {
        let log = AuditLog::new(4);
        for i in 0..6 {
            log.record(AuditEvent::SessionCreated {
                session_id: i.to_string(),
                knowledge_graph: "test".to_string(),
                timestamp: Instant::now(),
            });
        }
        // 6 total events recorded, logical_len should reflect 6
        // even though physical buffer is smaller
        assert_eq!(log.logical_len(), 6);
    }

    #[test]
    fn test_audit_clear_resets_drain_offset() {
        let log = AuditLog::new(4);
        // Fill past capacity to trigger drain
        for i in 0..6 {
            log.record(AuditEvent::SessionCreated {
                session_id: i.to_string(),
                knowledge_graph: "test".to_string(),
                timestamp: Instant::now(),
            });
        }
        assert_eq!(log.logical_len(), 6);

        // Clear should reset everything
        log.clear();
        assert_eq!(log.logical_len(), 0);
        assert!(log.is_empty());

        // After clear, new events should start from 0
        log.record(AuditEvent::SessionCreated {
            session_id: "99".to_string(),
            knowledge_graph: "test".to_string(),
            timestamp: Instant::now(),
        });
        assert_eq!(log.logical_len(), 1);

        // events_since(0) should return the new event
        let events = log.events_since(0);
        assert_eq!(events.len(), 1);
    }

    // === Ephemeral fact count ===

    #[test]
    fn test_ephemeral_counts() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        mgr.insert_ephemeral(
            &id,
            "edge",
            vec![make_tuple(vec![1, 2]), make_tuple(vec![2, 3])],
        )
        .unwrap();
        mgr.insert_ephemeral(&id, "node", vec![make_tuple(vec![1])])
            .unwrap();

        mgr.with_session(&id, |s| {
            assert_eq!(s.ephemeral_fact_count(), 3);
            assert_eq!(s.ephemeral_rule_count(), 0);
        })
        .unwrap();
    }

    // === Mixed writes (persistent + ephemeral in same session) ===

    #[test]
    fn test_session_facts_with_multiple_relations() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        // Insert into multiple relations
        mgr.insert_ephemeral(&id, "a", vec![make_tuple(vec![1])])
            .unwrap();
        mgr.insert_ephemeral(&id, "b", vec![make_tuple(vec![2])])
            .unwrap();
        mgr.insert_ephemeral(&id, "c", vec![make_tuple(vec![3])])
            .unwrap();

        let facts = mgr.get_session_facts(&id).unwrap();
        assert_eq!(facts.len(), 3);

        let relations: std::collections::HashSet<String> =
            facts.iter().map(|(r, _)| r.clone()).collect();
        assert!(relations.contains("a"));
        assert!(relations.contains("b"));
        assert!(relations.contains("c"));
    }

    // === Provenance ===

    // === Relation name validation ===

    #[test]
    fn test_insert_ephemeral_empty_relation_name() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();
        let result = mgr.insert_ephemeral(&id, "", vec![make_tuple(vec![1])]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("empty"));
    }

    #[test]
    fn test_insert_ephemeral_whitespace_relation_name() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();
        let result = mgr.insert_ephemeral(&id, "  ", vec![make_tuple(vec![1])]);
        assert!(result.is_err());
    }

    #[test]
    fn test_retract_ephemeral_empty_relation_name() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();
        let result = mgr.retract_ephemeral(&id, "", vec![make_tuple(vec![1])]);
        assert!(result.is_err());
    }

    // === Deterministic ordering ===

    #[test]
    fn test_session_facts_deterministic_order() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        // Insert in non-alphabetical order
        mgr.insert_ephemeral(&id, "zebra", vec![make_tuple(vec![1])])
            .unwrap();
        mgr.insert_ephemeral(&id, "alpha", vec![make_tuple(vec![2])])
            .unwrap();
        mgr.insert_ephemeral(&id, "middle", vec![make_tuple(vec![3])])
            .unwrap();

        let facts = mgr.get_session_facts(&id).unwrap();
        let relations: Vec<&str> = facts.iter().map(|(r, _)| r.as_str()).collect();
        assert_eq!(relations, vec!["alpha", "middle", "zebra"]);
    }

    #[test]
    fn test_metadata_ephemeral_sources_sorted() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        mgr.insert_ephemeral(&id, "zebra", vec![make_tuple(vec![1])])
            .unwrap();
        mgr.insert_ephemeral(&id, "alpha", vec![make_tuple(vec![2])])
            .unwrap();

        let meta = mgr.get_query_metadata(&id).unwrap();
        assert_eq!(meta.ephemeral_sources, vec!["alpha", "zebra"]);
    }

    #[test]
    fn test_provenance_serialization() {
        // Verify Provenance serializes to lowercase strings
        let p = Provenance::Persistent;
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(json, "\"persistent\"");

        let e = Provenance::Ephemeral;
        let json = serde_json::to_string(&e).unwrap();
        assert_eq!(json, "\"ephemeral\"");

        let m = Provenance::Mixed;
        let json = serde_json::to_string(&m).unwrap();
        assert_eq!(json, "\"mixed\"");
    }

    #[test]
    fn test_provenance_deserialization() {
        let p: Provenance = serde_json::from_str("\"persistent\"").unwrap();
        assert_eq!(p, Provenance::Persistent);

        let e: Provenance = serde_json::from_str("\"ephemeral\"").unwrap();
        assert_eq!(e, Provenance::Ephemeral);
    }

    // =========================================================================
    // Stress Tests: High-Concurrency Session Management
    // =========================================================================

    #[test]
    fn stress_100_concurrent_sessions() {
        let mgr = Arc::new(SessionManager::default());
        let mut handles = vec![];

        for i in 0..100 {
            let mgr = Arc::clone(&mgr);
            handles.push(std::thread::spawn(move || {
                let id = mgr.create_session(&format!("kg{}", i % 5)).unwrap();
                // Each session inserts its own facts
                mgr.insert_ephemeral(&id, "data", vec![make_tuple(vec![i])])
                    .unwrap();
                id
            }));
        }

        let ids: Vec<SessionId> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All IDs unique
        let mut unique = ids.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), 100);

        assert_eq!(mgr.session_count(), 100);

        // Verify each session has exactly 1 fact
        for id in &ids {
            let facts = mgr.get_session_facts(id).unwrap();
            assert_eq!(facts.len(), 1);
        }
    }

    #[test]
    fn stress_concurrent_insert_retract_same_session() {
        let mgr = Arc::new(SessionManager::default());
        let id = mgr.create_session("default").unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(50));
        let mut handles = vec![];

        // 25 inserters + 25 retractors hitting the same session
        for i in 0..25 {
            let mgr = Arc::clone(&mgr);
            let barrier = Arc::clone(&barrier);
            let id = id.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                mgr.insert_ephemeral(&id, "data", vec![make_tuple(vec![i])])
                    .unwrap();
            }));
        }

        for i in 0..25 {
            let mgr = Arc::clone(&mgr);
            let barrier = Arc::clone(&barrier);
            let id = id.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                // Try to retract (may or may not exist yet)
                let _ = mgr.retract_ephemeral(&id, "data", vec![make_tuple(vec![i])]);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // No crash, session still accessible
        let facts = mgr.get_session_facts(&id).unwrap();
        // Facts count is non-deterministic due to race, but must be valid
        assert!(facts.len() <= 25);
    }

    #[test]
    fn stress_large_scale_ephemeral_facts() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        // Insert 1000 facts in batches
        for batch in 0..10 {
            let tuples: Vec<Tuple> = (0..100)
                .map(|i| make_tuple(vec![batch * 100 + i]))
                .collect();
            mgr.insert_ephemeral(&id, "big_relation", tuples).unwrap();
        }

        let facts = mgr.get_session_facts(&id).unwrap();
        assert_eq!(facts.len(), 1000);

        // Retract half
        let retract_tuples: Vec<Tuple> = (0..500).map(|i| make_tuple(vec![i])).collect();
        let retracted = mgr
            .retract_ephemeral(&id, "big_relation", retract_tuples)
            .unwrap();
        assert_eq!(retracted, 500);

        let facts = mgr.get_session_facts(&id).unwrap();
        assert_eq!(facts.len(), 500);
    }

    #[test]
    fn stress_session_isolation_under_load() {
        let mgr = Arc::new(SessionManager::default());
        let num_sessions = 50;

        // Create sessions in parallel
        let mut handles = vec![];
        for i in 0..num_sessions {
            let mgr = Arc::clone(&mgr);
            handles.push(std::thread::spawn(move || {
                let id = mgr.create_session("default").unwrap();
                // Each session inserts unique facts in unique relations
                mgr.insert_ephemeral(
                    &id,
                    &format!("rel_{i}"),
                    vec![make_tuple(vec![i * 100, i * 100 + 1])],
                )
                .unwrap();
                // Also insert into shared relation with unique value
                mgr.insert_ephemeral(&id, "shared", vec![make_tuple(vec![i])])
                    .unwrap();
                id
            }));
        }

        let ids: Vec<SessionId> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Verify isolation: each session sees only its own facts
        for (idx, id) in ids.iter().enumerate() {
            let facts = mgr.get_session_facts(id).unwrap();
            assert_eq!(facts.len(), 2, "Session {idx} should have exactly 2 facts");

            // Verify the unique relation fact
            let unique_rel_facts: Vec<_> = facts
                .iter()
                .filter(|(r, _)| r == &format!("rel_{idx}"))
                .collect();
            assert_eq!(unique_rel_facts.len(), 1);

            // Verify the shared relation fact has only this session's value
            let shared_facts: Vec<_> = facts.iter().filter(|(r, _)| r == "shared").collect();
            assert_eq!(shared_facts.len(), 1);
        }
    }

    #[test]
    fn stress_rapid_create_destroy_cycle() {
        let mgr = Arc::new(SessionManager::default());
        let mut handles = vec![];

        for i in 0..100 {
            let mgr = Arc::clone(&mgr);
            handles.push(std::thread::spawn(move || {
                let id = mgr.create_session("default").unwrap();
                mgr.insert_ephemeral(&id, "temp", vec![make_tuple(vec![i])])
                    .unwrap();
                mgr.close_session(&id).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All sessions should be closed
        assert_eq!(mgr.session_count(), 0);
    }

    #[test]
    fn stress_concurrent_metadata_reads_during_writes() {
        let mgr = Arc::new(SessionManager::default());
        let id = mgr.create_session("default").unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(20));
        let mut handles = vec![];

        // 10 writers
        for i in 0..10 {
            let mgr = Arc::clone(&mgr);
            let barrier = Arc::clone(&barrier);
            let id = id.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                mgr.insert_ephemeral(&id, &format!("rel_{i}"), vec![make_tuple(vec![i])])
                    .unwrap();
            }));
        }

        // 10 metadata readers
        for _ in 0..10 {
            let mgr = Arc::clone(&mgr);
            let barrier = Arc::clone(&barrier);
            let id = id.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                // Should never panic, even mid-write
                let meta = mgr.get_query_metadata(&id).unwrap();
                // has_ephemeral may or may not be true depending on timing
                let _ = meta.has_ephemeral;
                let _ = meta.warnings.len();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn stress_multiple_kg_switches() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("kg0").unwrap();

        for i in 0..100 {
            mgr.insert_ephemeral(&id, "data", vec![make_tuple(vec![i])])
                .unwrap();
            mgr.switch_kg(&id, &format!("kg{}", (i + 1) % 5)).unwrap();
            // After switch, session should be clean
            assert!(mgr.is_session_clean(&id).unwrap());
        }
    }

    #[test]
    fn stress_ephemeral_dedup_at_scale() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        // Insert same 100 facts 10 times each
        for _ in 0..10 {
            let tuples: Vec<Tuple> = (0..100).map(|i| make_tuple(vec![i])).collect();
            mgr.insert_ephemeral(&id, "data", tuples).unwrap();
        }

        // Should be exactly 100 (deduped)
        let facts = mgr.get_session_facts(&id).unwrap();
        assert_eq!(facts.len(), 100);
    }

    #[test]
    fn stress_many_relations_per_session() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        // Insert into 200 different relations
        for i in 0..200 {
            mgr.insert_ephemeral(
                &id,
                &format!("relation_{i}"),
                vec![make_tuple(vec![i, i + 1])],
            )
            .unwrap();
        }

        let facts = mgr.get_session_facts(&id).unwrap();
        assert_eq!(facts.len(), 200);

        // Verify dirty with correct source count
        let meta = mgr.get_query_metadata(&id).unwrap();
        assert!(meta.has_ephemeral);
        assert_eq!(meta.ephemeral_sources.len(), 200);
    }

    #[test]
    fn stress_audit_log_under_load() {
        let mgr = Arc::new(SessionManager::default());
        let mut handles = vec![];

        for i in 0..20 {
            let mgr = Arc::clone(&mgr);
            handles.push(std::thread::spawn(move || {
                let id = mgr.create_session("default").unwrap();
                for j in 0..5 {
                    mgr.insert_ephemeral(&id, "data", vec![make_tuple(vec![i * 5 + j])])
                        .unwrap();
                }
                mgr.close_session(&id).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Audit log should have captured events (exact count depends on capacity)
        let events = mgr.audit_log().events();
        assert!(!events.is_empty());
    }

    #[test]
    fn stress_session_facts_and_rules_combined() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        // Add 50 ephemeral facts
        for i in 0..50 {
            mgr.insert_ephemeral(&id, "edge", vec![make_tuple(vec![i, i + 1])])
                .unwrap();
        }

        // Add 10 ephemeral rules
        for i in 0..10 {
            let rule = crate::ast::Rule {
                head: crate::ast::Atom {
                    relation: format!("derived_{i}"),
                    args: vec![
                        crate::ast::Term::Variable("X".to_string()),
                        crate::ast::Term::Variable("Y".to_string()),
                    ],
                },
                body: vec![crate::ast::BodyPredicate::Positive(crate::ast::Atom {
                    relation: "edge".to_string(),
                    args: vec![
                        crate::ast::Term::Variable("X".to_string()),
                        crate::ast::Term::Variable("Y".to_string()),
                    ],
                })],
            };
            mgr.add_ephemeral_rule(&id, rule, format!("derived_{i}(X, Y) <- edge(X, Y)"))
                .unwrap();
        }

        mgr.with_session(&id, |s| {
            assert_eq!(s.ephemeral_fact_count(), 50);
            assert_eq!(s.ephemeral_rule_count(), 10);
            assert!(!s.is_clean());
        })
        .unwrap();

        // Verify metadata reflects both facts and rules
        let meta = mgr.get_query_metadata(&id).unwrap();
        assert!(meta.has_ephemeral);
        assert!(meta.ephemeral_sources.contains(&"edge".to_string()));
    }

    #[test]
    fn stress_retract_all_then_reinsert() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        let tuples: Vec<Tuple> = (0..100).map(|i| make_tuple(vec![i])).collect();

        // Insert all
        mgr.insert_ephemeral(&id, "data", tuples.clone()).unwrap();
        assert_eq!(mgr.get_session_facts(&id).unwrap().len(), 100);

        // Retract all
        let retracted = mgr.retract_ephemeral(&id, "data", tuples.clone()).unwrap();
        assert_eq!(retracted, 100);
        assert!(mgr.is_session_clean(&id).unwrap());

        // Reinsert all
        mgr.insert_ephemeral(&id, "data", tuples).unwrap();
        assert_eq!(mgr.get_session_facts(&id).unwrap().len(), 100);
        assert!(!mgr.is_session_clean(&id).unwrap());
    }

    #[test]
    fn stress_session_max_limit() {
        let config = SessionConfig {
            max_sessions: 10,
            idle_timeout_secs: 0,
            ..Default::default()
        };
        let mgr = SessionManager::new(config);

        // Create up to the limit
        let mut ids = vec![];
        for _ in 0..10 {
            ids.push(mgr.create_session("default").unwrap());
        }

        // 11th session should fail
        let result = mgr.create_session("default");
        assert!(result.is_err());

        // Close one and create again
        mgr.close_session(&ids[0]).unwrap();
        let new_id = mgr.create_session("default").unwrap();
        assert_ne!(new_id, ids[0]);
    }

    #[test]
    fn stress_concurrent_different_operations() {
        // Mix of creates, inserts, retracts, metadata reads, closes
        let mgr = Arc::new(SessionManager::default());
        let barrier = Arc::new(std::sync::Barrier::new(40));
        let mut handles = vec![];

        // 10 creators that immediately close
        for _ in 0..10 {
            let mgr = Arc::clone(&mgr);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let id = mgr.create_session("default").unwrap();
                mgr.close_session(&id).unwrap();
            }));
        }

        // Pre-create sessions for the other threads to use
        let shared_ids: Vec<SessionId> = (0..10)
            .map(|_| mgr.create_session("default").unwrap())
            .collect();

        // 10 inserters
        for i in 0..10 {
            let mgr = Arc::clone(&mgr);
            let barrier = Arc::clone(&barrier);
            let id = shared_ids[i].clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                mgr.insert_ephemeral(&id, "data", vec![make_tuple(vec![i as i64])])
                    .unwrap();
            }));
        }

        // 10 metadata readers
        for i in 0..10 {
            let mgr = Arc::clone(&mgr);
            let barrier = Arc::clone(&barrier);
            let id = shared_ids[i].clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let _ = mgr.get_query_metadata(&id);
            }));
        }

        // 10 stats readers
        for _ in 0..10 {
            let mgr = Arc::clone(&mgr);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let _ = mgr.stats();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // No panics, system is consistent
        let count = mgr.session_count();
        assert!(count <= 20); // At most 10 shared + some creators not yet closed
    }

    #[test]
    fn stress_overshadow_detection_many_rules() {
        let mut session = Session::new("test-1".to_string(), "default".to_string());

        // Add 50 ephemeral rules for different relations
        for i in 0..50 {
            session.add_ephemeral_rule(
                crate::ast::Rule {
                    head: crate::ast::Atom {
                        relation: format!("derived_{i}"),
                        args: vec![crate::ast::Term::Variable("X".to_string())],
                    },
                    body: vec![crate::ast::BodyPredicate::Positive(crate::ast::Atom {
                        relation: "edge".to_string(),
                        args: vec![crate::ast::Term::Variable("X".to_string())],
                    })],
                },
                format!("derived_{i}(X) <- edge(X)"),
            );
        }

        // Check overlap with 50 persistent rules (half overlap, half don't)
        let persistent_heads: Vec<String> = (0..50)
            .map(|i| {
                if i < 25 {
                    format!("derived_{i}")
                } else {
                    format!("persistent_{i}")
                }
            })
            .collect();

        let overshadowed = session.detect_overshadowed_rules(&persistent_heads);
        assert_eq!(overshadowed.len(), 25);
    }

    #[test]
    fn stress_ephemeral_vector_data() {
        use crate::value::Value;

        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        // Insert 100 vector tuples (simulating embedding queries)
        let tuples: Vec<Tuple> = (0..100)
            .map(|i| {
                Tuple::new(vec![
                    Value::string(&format!("doc_{i}")),
                    Value::vector(vec![i as f32 * 0.1, i as f32 * 0.2, i as f32 * 0.3]),
                ])
            })
            .collect();

        mgr.insert_ephemeral(&id, "query_embedding", tuples)
            .unwrap();

        let facts = mgr.get_session_facts(&id).unwrap();
        assert_eq!(facts.len(), 100);

        // Verify specific vector fact
        let first = &facts[0].1;
        assert_eq!(first.arity(), 2);
    }

    #[test]
    fn stress_clear_and_reuse_cycle() {
        let mgr = SessionManager::default();
        let id = mgr.create_session("default").unwrap();

        for cycle in 0..50 {
            // Insert facts
            mgr.insert_ephemeral(&id, "data", vec![make_tuple(vec![cycle, cycle + 1])])
                .unwrap();
            assert!(!mgr.is_session_clean(&id).unwrap());

            // Clear
            mgr.clear_session(&id).unwrap();
            assert!(mgr.is_session_clean(&id).unwrap());
            assert_eq!(mgr.get_session_facts(&id).unwrap().len(), 0);
        }
    }

    #[test]
    fn stress_concurrent_sessions_same_kg() {
        // Simulate many agent sessions on same KG (production scenario)
        let mgr = Arc::new(SessionManager::default());
        let barrier = Arc::new(std::sync::Barrier::new(50));
        let mut handles = vec![];

        for i in 0..50 {
            let mgr = Arc::clone(&mgr);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let id = mgr.create_session("production_kg").unwrap();
                barrier.wait();

                // Each agent: insert query embedding, add rule, check metadata
                mgr.insert_ephemeral(&id, "query_embedding", vec![make_tuple(vec![i])])
                    .unwrap();

                let rule = crate::ast::Rule {
                    head: crate::ast::Atom {
                        relation: "relevant".to_string(),
                        args: vec![crate::ast::Term::Variable("X".to_string())],
                    },
                    body: vec![crate::ast::BodyPredicate::Positive(crate::ast::Atom {
                        relation: "doc".to_string(),
                        args: vec![crate::ast::Term::Variable("X".to_string())],
                    })],
                };
                mgr.add_ephemeral_rule(&id, rule, "relevant(X) <- doc(X)".to_string())
                    .unwrap();

                let meta = mgr.get_query_metadata(&id).unwrap();
                assert!(meta.has_ephemeral);

                // Cleanup
                mgr.close_session(&id).unwrap();
                id
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(mgr.session_count(), 0);
    }

    // === Per-session resource limits ===

    // === Regression tests for close_sessions_for_kg ===

    #[test]
    fn test_close_sessions_for_kg_removes_matching() {
        let mgr = SessionManager::new(SessionConfig::default());
        let id1 = mgr.create_session("kg_a").unwrap();
        let id2 = mgr.create_session("kg_a").unwrap();
        let id3 = mgr.create_session("kg_b").unwrap();

        assert_eq!(mgr.session_count(), 3);

        let closed = mgr.close_sessions_for_kg("kg_a");
        assert_eq!(closed, 2);
        assert_eq!(mgr.session_count(), 1);

        // The kg_b session should still exist
        assert!(mgr.with_session(&id3, |_| {}).is_ok());
        // The kg_a sessions should be gone
        assert!(mgr.with_session(&id1, |_| {}).is_err());
        assert!(mgr.with_session(&id2, |_| {}).is_err());
    }

    #[test]
    fn test_close_sessions_for_kg_no_match() {
        let mgr = SessionManager::new(SessionConfig::default());
        mgr.create_session("kg_x").unwrap();

        let closed = mgr.close_sessions_for_kg("nonexistent");
        assert_eq!(closed, 0);
        assert_eq!(mgr.session_count(), 1);
    }

    #[test]
    fn test_close_sessions_for_kg_empty() {
        let mgr = SessionManager::new(SessionConfig::default());
        let closed = mgr.close_sessions_for_kg("any");
        assert_eq!(closed, 0);
    }

    #[test]
    fn test_ephemeral_fact_limit_enforced() {
        let config = SessionConfig {
            max_ephemeral_facts: 5,
            max_ephemeral_rules: 100,
            ..Default::default()
        };
        let mgr = SessionManager::new(config);
        let id = mgr.create_session("default").unwrap();

        // Insert 3 facts — OK
        mgr.insert_ephemeral(
            &id,
            "edge",
            vec![
                make_tuple(vec![1, 2]),
                make_tuple(vec![2, 3]),
                make_tuple(vec![3, 4]),
            ],
        )
        .unwrap();

        // Insert 3 more — exceeds limit of 5
        let result = mgr.insert_ephemeral(
            &id,
            "edge",
            vec![
                make_tuple(vec![4, 5]),
                make_tuple(vec![5, 6]),
                make_tuple(vec![6, 7]),
            ],
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("limit exceeded"));

        // Original 3 facts should still be intact
        let facts = mgr.get_session_facts(&id).unwrap();
        assert_eq!(facts.len(), 3);
    }

    #[test]
    fn test_ephemeral_fact_limit_zero_means_unlimited() {
        let config = SessionConfig {
            max_ephemeral_facts: 0,
            ..Default::default()
        };
        let mgr = SessionManager::new(config);
        let id = mgr.create_session("default").unwrap();

        // Insert many facts — should succeed with 0 (unlimited)
        let tuples: Vec<Tuple> = (0..1000).map(|i| make_tuple(vec![i])).collect();
        mgr.insert_ephemeral(&id, "data", tuples).unwrap();
        assert_eq!(mgr.get_session_facts(&id).unwrap().len(), 1000);
    }

    #[test]
    fn test_ephemeral_rule_limit_enforced() {
        let config = SessionConfig {
            max_ephemeral_rules: 2,
            max_ephemeral_facts: 100_000,
            ..Default::default()
        };
        let mgr = SessionManager::new(config);
        let id = mgr.create_session("default").unwrap();

        let make_rule = |name: &str| crate::ast::Rule {
            head: crate::ast::Atom {
                relation: name.to_string(),
                args: vec![],
            },
            body: vec![],
        };

        // Add 2 rules — OK
        mgr.add_ephemeral_rule(&id, make_rule("r1"), "r1() <-".to_string())
            .unwrap();
        mgr.add_ephemeral_rule(&id, make_rule("r2"), "r2() <-".to_string())
            .unwrap();

        // 3rd rule should fail
        let result = mgr.add_ephemeral_rule(&id, make_rule("r3"), "r3() <-".to_string());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("limit exceeded"));

        // Only 2 rules should exist
        mgr.with_session(&id, |s| {
            assert_eq!(s.ephemeral_rule_count(), 2);
        })
        .unwrap();
    }

    #[test]
    fn test_ephemeral_rule_limit_zero_means_unlimited() {
        let config = SessionConfig {
            max_ephemeral_rules: 0,
            ..Default::default()
        };
        let mgr = SessionManager::new(config);
        let id = mgr.create_session("default").unwrap();

        let make_rule = |name: &str| crate::ast::Rule {
            head: crate::ast::Atom {
                relation: name.to_string(),
                args: vec![],
            },
            body: vec![],
        };

        // Should succeed with unlimited
        for i in 0..100 {
            mgr.add_ephemeral_rule(&id, make_rule(&format!("r{i}")), format!("r{i}() <-"))
                .unwrap();
        }

        mgr.with_session(&id, |s| {
            assert_eq!(s.ephemeral_rule_count(), 100);
        })
        .unwrap();
    }
}
