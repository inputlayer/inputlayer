//! Authenticated engine connections, pooled per knowledge graph.
//!
//! The engine's WebSocket protocol is strictly request/response with no
//! request ids, so one socket cannot multiplex concurrent requests. A
//! single mutex-guarded connection per KG would serialize every
//! conversation sharing that KG behind one socket; instead each request
//! checks out an EXCLUSIVE connection and returns it when done. Idle
//! connections are keyed by KG, so `.kg use` runs once per connection,
//! never per request, and a connection can never execute against the
//! wrong KG.
//!
//! Health: an idle connection keeps receiving server pushes (change
//! notifications, pings, and eventually an idle-timeout close). Checkout
//! drains those without blocking and discards a closed socket; idle
//! connections older than `MAX_IDLE` (well under the engine's 5 minute
//! idle timeout) are discarded unused. If a reused connection still turns
//! out dead on its first command, the command is retried once on a fresh
//! connection - safe because every program the gateway sends is
//! idempotent (set-semantics inserts, conditional deletes, queries).

use anyhow::{Context, Result};
use inputlayer_ontology_client::ws::{Disconnected, Engine, QueryResult};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Idle connections kept per KG; extra returns are closed.
const MAX_IDLE_PER_KG: usize = 8;
/// Idle connections older than this are discarded rather than reused.
const MAX_IDLE: Duration = Duration::from_secs(60);
/// Connections older than this are retired (engine lifetime caps).
const MAX_AGE: Duration = Duration::from_secs(3600);

struct Idle {
    engine: Engine,
    created: Instant,
    since: Instant,
}

pub struct EnginePool {
    url: String,
    api_key: String,
    idle: Mutex<HashMap<String, Vec<Idle>>>,
}

impl EnginePool {
    pub fn new(url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            api_key: api_key.into(),
            idle: Mutex::new(HashMap::new()),
        }
    }

    /// An exclusive, authenticated connection bound to `kg`.
    pub async fn checkout(&self, kg: &str) -> Result<PooledEngine<'_>> {
        if let Some((engine, created)) = self.take_idle(kg) {
            return Ok(PooledEngine {
                pool: self,
                kg: kg.to_string(),
                engine: Some(engine),
                created,
                reused: true,
                fresh_command: true,
                in_flight: false,
            });
        }
        let engine = self.open(kg).await?;
        Ok(PooledEngine {
            pool: self,
            kg: kg.to_string(),
            engine: Some(engine),
            created: Instant::now(),
            reused: false,
            fresh_command: true,
            in_flight: false,
        })
    }

    /// Number of idle connections held for `kg` (observability, tests).
    pub fn idle_count(&self, kg: &str) -> usize {
        self.lock().get(kg).map_or(0, Vec::len)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Vec<Idle>>> {
        self.idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn take_idle(&self, kg: &str) -> Option<(Engine, Instant)> {
        let mut map = self.lock();
        let list = map.get_mut(kg)?;
        while let Some(mut idle) = list.pop() {
            let fresh = idle.since.elapsed() < MAX_IDLE && idle.created.elapsed() < MAX_AGE;
            if fresh && idle.engine.drain_idle() {
                return Some((idle.engine, idle.created));
            }
        }
        None
    }

    fn give_back(&self, kg: &str, engine: Engine, created: Instant) {
        if created.elapsed() >= MAX_AGE {
            return;
        }
        let mut map = self.lock();
        let list = map.entry(kg.to_string()).or_default();
        if list.len() < MAX_IDLE_PER_KG {
            list.push(Idle {
                engine,
                created,
                since: Instant::now(),
            });
        }
    }

    async fn open(&self, kg: &str) -> Result<Engine> {
        let mut engine = Engine::connect(&self.url, &self.api_key)
            .await
            .context("engine unreachable")?;
        engine
            .execute(&format!(".kg use {kg}"))
            .await
            .with_context(|| format!("knowledge graph '{kg}' not available"))?;
        Ok(engine)
    }
}

/// A checked-out connection. Returned to the pool on drop unless a
/// transport failure poisoned it.
pub struct PooledEngine<'a> {
    pool: &'a EnginePool,
    kg: String,
    engine: Option<Engine>,
    created: Instant,
    reused: bool,
    /// No command has run on this checkout yet: the only point at which a
    /// stale reused socket is retried on a fresh one.
    fresh_command: bool,
    /// A command was sent and its response not yet fully read. A checkout
    /// dropped in this state (the request future was cancelled) holds a
    /// socket with a stale response queued: it must be closed, never
    /// handed to the next request.
    in_flight: bool,
}

impl PooledEngine<'_> {
    pub async fn execute(&mut self, program: &str) -> Result<QueryResult> {
        self.in_flight = true;
        let result = self.execute_inner(program).await;
        self.in_flight = false;
        result
    }

    async fn execute_inner(&mut self, program: &str) -> Result<QueryResult> {
        let first = std::mem::replace(&mut self.fresh_command, false);
        let engine = self
            .engine
            .as_mut()
            .context("engine connection already failed")?;
        match engine.execute(program).await {
            Ok(result) => Ok(result),
            Err(err) if err.downcast_ref::<Disconnected>().is_some() => {
                self.engine = None;
                if !(first && self.reused) {
                    return Err(err);
                }
                let mut engine = self.pool.open(&self.kg).await?;
                self.created = Instant::now();
                self.reused = false;
                let result = engine.execute(program).await;
                if result
                    .as_ref()
                    .err()
                    .is_none_or(|e| e.downcast_ref::<Disconnected>().is_none())
                {
                    self.engine = Some(engine);
                }
                result
            }
            Err(err) => {
                // A timeout leaves the socket mid-response: never reuse it.
                if err.to_string().contains("server response timeout") {
                    self.engine = None;
                }
                Err(err)
            }
        }
    }
}

impl Drop for PooledEngine<'_> {
    fn drop(&mut self) {
        if self.in_flight {
            return;
        }
        if let Some(engine) = self.engine.take() {
            self.pool.give_back(&self.kg, engine, self.created);
        }
    }
}
