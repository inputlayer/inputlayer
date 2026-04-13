//! Teaching Agent - Claude-powered guided onboarding for InputLayer.
//!
//! Provides an interactive teaching experience where Claude guides users
//! through IQL concepts using curated examples. Each example has its own
//! knowledge graph and a system prompt that teaches the relevant concept
//! while highlighting InputLayer's value proposition.
//!
//! The agent communicates with the GUI via WebSocket (`.agent` meta command)
//! and calls the Claude API server-side (API key never exposed to the client).

pub mod claude;
pub mod examples;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::RwLock;

/// Configuration for the teaching agent.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// Claude API key (from INPUTLAYER_CLAUDE_API_KEY env var)
    pub api_key: Option<String>,
    /// Claude model to use
    pub model: String,
    /// Max tokens per response
    pub max_tokens: usize,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            api_key: std::env::var("INPUTLAYER_CLAUDE_API_KEY").ok(),
            model: "claude-haiku-4-5-20251001".to_string(),
            max_tokens: 800,
        }
    }
}

impl AgentConfig {
    /// Check if the agent is available (has an API key).
    pub fn is_available(&self) -> bool {
        self.api_key.as_ref().is_some_and(|k| !k.is_empty())
    }
}

/// A single message in a conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String, // "user" or "assistant"
    pub content: String,
}

/// A conversation session with the teaching agent.
#[derive(Debug)]
pub struct AgentSession {
    /// The example this session is teaching
    pub example_id: String,
    /// Current step index in the scripted lesson
    pub current_step: usize,
    /// Conversation history
    pub messages: Vec<Message>,
    /// Current KG context (relations and their tuple counts)
    pub kg_context: String,
}

impl AgentSession {
    pub fn new(example_id: &str) -> Self {
        Self {
            example_id: example_id.to_string(),
            current_step: 0,
            messages: Vec::new(),
            kg_context: String::new(),
        }
    }

    /// Update the KG context (called after queries execute).
    pub fn update_context(&mut self, context: String) {
        self.kg_context = context;
    }

    /// Add a user message.
    pub fn add_user_message(&mut self, content: &str) {
        self.messages.push(Message {
            role: "user".to_string(),
            content: content.to_string(),
        });
    }

    /// Add an assistant message.
    pub fn add_assistant_message(&mut self, content: &str) {
        self.messages.push(Message {
            role: "assistant".to_string(),
            content: content.to_string(),
        });
    }
}

/// Agent response sent back to the GUI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResponse {
    /// The text content of the response
    pub content: String,
    /// Optional suggested query for the user to try
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_query: Option<String>,
    /// Whether this is the final chunk (for streaming)
    pub done: bool,
}

/// Concise IQL reference injected into every Claude system prompt so the agent
/// can answer questions about InputLayer's syntax, commands, and GUI.
pub const IQL_REFERENCE: &str = r"
## InputLayer Quick Reference

InputLayer is a knowledge graph engine with recursive IQL, incremental maintenance, and provenance.

### IQL Syntax
- `+fact(a, b)` - insert a fact
- `-fact(a, b)` - retract a fact
- `+head(X, Y) <- body(X, Z), body2(Z, Y)` - persistent rule (derived relation)
- `head(X, Y) <- body(X, Y)` - session rule (transient, not persisted)
- `?relation(X, Y)` - query (uppercase = variables, lowercase/quoted = constants, `_` = wildcard)
- `+bulk[(1,2), (3,4)]` - bulk insert
- `-fact(X, Y) <- fact(X, Y), X > 5` - conditional delete
- `!negated(X)` - negation (variable must appear positively elsewhere)
- Aggregations in head: `count<X>`, `sum<X>`, `min<X>`, `max<X>`, `avg<X>`
- Arithmetic: `+`, `-`, `*`, `/`, `%`; Comparisons: `=`, `!=`, `<`, `<=`, `>`, `>=`
- Vectors: `[1.0, 2.0, 3.0]`; Functions: `cosine(v1,v2)`, `euclidean(v1,v2)`, `dot(v1,v2)`
- Schema: `+rel(col1: int, col2: string, col3: vector)`
- Types: `int`, `float`, `string`, `bool`, `timestamp`, `vector`

### Meta Commands
- `.why ?query(X)` - show proof tree (derivation chain of rules + facts)
- `.why full ?query(X)` - full mode (enumerates all aggregation contributors)
- `.why_not relation(val1, val2)` - explain why a tuple was NOT derived (shows exact blocker)
- `.debug ?query(X)` - show query plan without executing
- `.rel` - list all relations with schemas and row counts
- `.rel <name>` - describe a relation (schema + sample data)
- `.rel drop <name>` - drop a relation
- `.rule` / `.rule list` - list all persistent rules
- `.rule def <name>` - show rule definition (clauses)
- `.rule drop <name>` - delete a rule
- `.rule edit <name> <n> <clause>` - edit clause #n
- `.kg` / `.kg list` / `.kg create <n>` / `.kg use <n>` / `.kg drop <n>` - knowledge graph management
- `.session` / `.session clear` - session rule management
- `.index list` / `.index create <name> on <rel>(<col>) [metric cosine]` - HNSW vector index management
- `.index stats <name>` / `.index rebuild <name>` / `.index drop <name>`
- `.load <file.iql>` - execute an IQL script file
- `.status` - system status
- `.compact` - compact storage

### Studio GUI Features
- **Query Editor** (left panel): write and execute IQL statements (Cmd/Ctrl+Enter to run)
- **Results Panel** (right panel): shows query results as a table
- **Proof Trees**: `.why` results render as interactive expandable trees showing each derivation step
- **Learn Panel** (sidebar): guided interactive tutorials that build KGs step by step
- **Relations Browser**: `.rel` shows all relations; click a relation name to see its data
- **KG Switcher** (header): switch between knowledge graphs
- **Docs** (sidebar tab): searchable documentation

### Key Concepts
- **Incremental maintenance**: when facts change, only affected derived relations recompute (not everything)
- **Correct retraction**: removing a fact only retracts conclusions when ALL supporting derivation paths are gone
- **Provenance**: every derived fact has a traceable proof tree (`.why`) showing exactly how it was derived
- **Negative explanation**: `.why_not` shows the exact rule condition that blocked a derivation
- **Recursion**: rules can reference themselves (e.g., transitive closure: `+path(X,Z) <- path(X,Y), edge(Y,Z)`)
- **Stratified negation**: negation is allowed but not through recursive cycles
";

/// Manages agent sessions across WebSocket connections.
#[derive(Debug, Default)]
pub struct AgentManager {
    pub(crate) sessions: RwLock<HashMap<String, AgentSession>>,
    config: AgentConfig,
}

impl AgentManager {
    pub fn new(config: AgentConfig) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Check if the agent is available.
    pub fn is_available(&self) -> bool {
        self.config.is_available()
    }

    /// Get or create a session for a given session ID and example.
    pub async fn get_or_create_session(&self, session_id: &str, example_id: &str) -> String {
        let mut sessions = self.sessions.write().await;
        let key = format!("{session_id}:{example_id}");
        if !sessions.contains_key(&key) {
            sessions.insert(key.clone(), AgentSession::new(example_id));
        }
        key
    }

    /// Process a user message and get the agent's response.
    pub async fn process_message(
        &self,
        session_key: &str,
        user_message: &str,
        kg_context: &str,
    ) -> Result<AgentResponse, String> {
        let api_key = match self.config.api_key.as_ref() {
            Some(k) if !k.is_empty() => k,
            _ => {
                // No Claude API key - give a helpful response without AI
                return Ok(AgentResponse {
                    content: "I can't answer free-form questions without an AI backend configured. But you can continue the lesson by clicking the suggested query buttons above, or type `.agent next` to advance to the next step.".to_string(),
                    suggested_query: None,
                    done: true,
                });
            }
        };

        // Get the session and example
        let (system_prompt, messages) = {
            let mut sessions = self.sessions.write().await;
            let session = sessions.get_mut(session_key).ok_or("Session not found")?;

            session.update_context(kg_context.to_string());
            session.add_user_message(user_message);

            let example = examples::get_example(&session.example_id);
            let lesson_done = match &example {
                Some(ex) => session.current_step >= ex.step_count(),
                None => true,
            };
            let system = match example {
                Some(ex) => ex.build_system_prompt(&session.kg_context, lesson_done),
                None => format!(
                    "You are an InputLayer teaching assistant. Help the user learn the InputLayer Query Language (IQL). \
                     Current knowledge graph context:\n{kg_context}\n\n{IQL_REFERENCE}"
                ),
            };

            (system, session.messages.clone())
        };

        // Call Claude API
        let response = claude::call_claude(
            api_key,
            &self.config.model,
            &system_prompt,
            &messages,
            self.config.max_tokens,
        )
        .await?;

        // Parse suggested query from response (look for ```iql blocks)
        let suggested_query = extract_suggested_query(&response);

        // Store assistant response
        {
            let mut sessions = self.sessions.write().await;
            if let Some(session) = sessions.get_mut(session_key) {
                session.add_assistant_message(&response);
            }
        }

        Ok(AgentResponse {
            content: response,
            suggested_query,
            done: true,
        })
    }

    /// Start a new conversation for an example (resets history).
    pub async fn start_example(
        &self,
        session_key: &str,
        example_id: &str,
        _kg_context: &str,
    ) -> Result<AgentResponse, String> {
        // Reset session
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_key.to_string(), AgentSession::new(example_id));
        }

        let example = examples::get_example(example_id)
            .ok_or_else(|| format!("Unknown example: {example_id}"))?;

        // Return the first scripted step
        let content = example
            .step_message(0)
            .unwrap_or_else(|| "No steps defined for this example.".to_string());
        let suggested_query = example.steps.first().map(|s| s.iql.to_string());

        Ok(AgentResponse {
            content,
            suggested_query,
            done: false,
        })
    }

    /// Advance to the next step in the scripted lesson.
    pub async fn next_step(&self, session_key: &str) -> Result<AgentResponse, String> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(session_key).ok_or("Session not found")?;

        session.current_step += 1;

        let example = examples::get_example(&session.example_id).ok_or("Example not found")?;

        if session.current_step >= example.step_count() {
            return Ok(AgentResponse {
                content: "You've completed the lesson! Feel free to experiment with your own queries or ask me anything.".to_string(),
                suggested_query: None,
                done: true,
            });
        }

        let content = example
            .step_message(session.current_step)
            .unwrap_or_default();
        let suggested_query = example
            .steps
            .get(session.current_step)
            .map(|s| s.iql.to_string());
        let is_last_step = session.current_step + 1 >= example.step_count();

        Ok(AgentResponse {
            content,
            suggested_query,
            done: is_last_step,
        })
    }
}

/// Extract a suggested query from the agent's response.
/// Looks for ```iql or ```iql code blocks, or lines starting with ? or .why
fn extract_suggested_query(response: &str) -> Option<String> {
    // Look for fenced code blocks
    for marker in ["```iql", "```datalog", "```"] {
        if let Some(start) = response.find(marker) {
            let code_start = start + marker.len();
            if let Some(end) = response[code_start..].find("```") {
                let code = response[code_start..code_start + end].trim();
                // Find the first query line
                for line in code.lines() {
                    let trimmed = line.trim();
                    if trimmed.starts_with('?') || trimmed.starts_with(".why") {
                        return Some(trimmed.to_string());
                    }
                }
                // If no query line, return the whole code block
                if !code.is_empty() {
                    return Some(code.to_string());
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_suggested_query_iql_block() {
        let response = "Try this query:\n```iql\n?can_reach(\"new_york\", X)\n```\nThis will show all reachable cities.";
        assert_eq!(
            extract_suggested_query(response),
            Some("?can_reach(\"new_york\", X)".to_string())
        );
    }

    #[test]
    fn test_extract_suggested_query_iql_block_why() {
        let response = "Run this:\n```iql\n.why ?purchase_ok(\"team_a\", \"acme\", 3200)\n```";
        assert_eq!(
            extract_suggested_query(response),
            Some(".why ?purchase_ok(\"team_a\", \"acme\", 3200)".to_string())
        );
    }

    #[test]
    fn test_extract_suggested_query_none() {
        let response = "InputLayer uses recursive rules to derive conclusions.";
        assert_eq!(extract_suggested_query(response), None);
    }

    #[test]
    fn test_all_examples_have_steps() {
        let examples = examples::all_examples();
        assert!(examples.len() >= 7, "should have at least 7 examples");
        for ex in examples {
            assert!(!ex.steps.is_empty(), "{}: has no steps", ex.id);
            assert!(
                !ex.system_prompt.is_empty(),
                "{}: system_prompt is empty",
                ex.id
            );
            assert!(!ex.name.is_empty(), "{}: name is empty", ex.id);
            // Every step should have non-empty message and IQL
            for (i, step) in ex.steps.iter().enumerate() {
                assert!(
                    !step.message.is_empty(),
                    "{} step {}: message is empty",
                    ex.id,
                    i
                );
                assert!(!step.iql.is_empty(), "{} step {}: iql is empty", ex.id, i);
            }
        }
    }

    #[test]
    fn test_get_example_by_id() {
        assert!(examples::get_example("flights").is_some());
        assert!(examples::get_example("retraction").is_some());
        assert!(examples::get_example("provenance").is_some());
        assert!(examples::get_example("incremental").is_some());
        assert!(examples::get_example("rules_vectors").is_some());
        assert!(examples::get_example("agentic_ai").is_some());
        assert!(examples::get_example("schemas").is_some());
        assert!(examples::get_example("nonexistent").is_none());
    }

    #[test]
    fn test_flights_has_complete_lesson() {
        let ex = examples::get_example("flights").unwrap();
        assert!(
            ex.steps.len() >= 15,
            "flights should have at least 15 steps"
        );
        // Should teach facts, rules, queries, recursion, provenance, retraction
        let all_iql: String = ex
            .steps
            .iter()
            .map(|s| s.iql)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(all_iql.contains("direct_flight"), "should insert flights");
        assert!(all_iql.contains("can_reach"), "should define can_reach");
        assert!(all_iql.contains("<-"), "should have rules");
        assert!(all_iql.contains(".why"), "should teach provenance");
        assert!(
            all_iql.contains("-direct_flight"),
            "should teach retraction"
        );
    }

    #[test]
    fn test_step_message_format() {
        let ex = examples::get_example("flights").unwrap();
        let msg = ex.step_message(0).expect("first step should exist");
        assert!(
            msg.contains("```iql"),
            "step message should contain iql code block"
        );
        assert!(
            msg.contains("direct_flight"),
            "first step should insert a flight"
        );
    }

    #[test]
    fn test_agent_config_default() {
        let config = AgentConfig::default();
        assert_eq!(config.model, "claude-haiku-4-5-20251001");
        assert_eq!(config.max_tokens, 800);
    }

    #[test]
    fn test_agent_session_messages() {
        let mut session = AgentSession::new("flights");
        session.add_user_message("hello");
        session.add_assistant_message("hi there");
        assert_eq!(session.messages.len(), 2);
        assert_eq!(session.messages[0].role, "user");
        assert_eq!(session.messages[1].role, "assistant");
    }
}
