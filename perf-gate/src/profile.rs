//! Workload sizes. A profile fixes every parameter of every fixture; runs are
//! only comparable within one profile, and the run file records all of it.

use serde::Serialize;

/// Seed of every generated dataset.
pub const SEED: u64 = 42;

#[derive(Debug, Clone, Serialize)]
pub struct Profile {
    pub name: &'static str,
    pub cheap_query: QueryParams,
    pub bound_query: QueryParams,
    pub insert: InsertParams,
    pub delta_single: DeltaParams,
    pub delta_fanout: DeltaParams,
    pub delta_first: FirstDeltaParams,
    pub delta_keyed: KeyedParams,
    pub interference: InterferenceParams,
    pub shop_install: ShopParams,
    pub recursive_query: RecursiveParams,
    /// The engine suite's fixtures (not gated; see `fixtures::engine`).
    pub engine: EngineParams,
}

/// A warm read query: a serial latency phase, then a concurrent throughput phase.
#[derive(Debug, Clone, Serialize)]
pub struct QueryParams {
    pub nodes: u64,
    pub edges: usize,
    pub warmup: usize,
    pub serial: usize,
    pub clients: usize,
    pub per_client: usize,
}

/// Persisted inserts with the default (immediate) durability.
#[derive(Debug, Clone, Serialize)]
pub struct InsertParams {
    pub single: usize,
    pub writers: usize,
    pub per_writer: usize,
    pub batches: usize,
    pub batch_size: usize,
}

/// External writer to subscribed agents, written open-loop at a fixed interval
/// so a slow server cannot hide latency by slowing the writer down.
#[derive(Debug, Clone, Serialize)]
pub struct DeltaParams {
    pub nodes: u64,
    pub edges: usize,
    pub subscribers: usize,
    /// Even: each insert is followed by its retraction.
    pub writes: usize,
    pub interval_ms: u64,
}

/// Fresh agents, each timing its first write after subscribing and then a
/// warm one.
#[derive(Debug, Clone, Serialize)]
pub struct FirstDeltaParams {
    pub nodes: u64,
    pub edges: usize,
    /// Agents, one after another; each contributes one sample per series.
    pub cycles: usize,
    /// Agent quiet time before the warm write.
    pub settle_ms: u64,
}

/// Agents each subscribed to their own key of a rule over a graph of
/// `edges`, written open-loop at a fixed interval.
#[derive(Debug, Clone, Serialize)]
pub struct KeyedParams {
    pub edges: usize,
    /// Agents, one per key; at most one per hundred chains of three edges.
    pub keys: usize,
    pub writes: usize,
    pub interval_ms: u64,
}

/// One probe subscriber while a long request loops on another connection and
/// a slow consumer stops reading its socket.
#[derive(Debug, Clone, Serialize)]
pub struct InterferenceParams {
    pub delta: DeltaParams,
    /// Large results the slow consumer requests and never reads.
    pub slow_consumer_requests: usize,
}

/// The scenario suite's shop pack (`Size::Small`) installed into fresh
/// knowledge graphs.
#[derive(Debug, Clone, Serialize)]
pub struct ShopParams {
    /// Installs per round, each into a graph of its own.
    pub installs: usize,
}

/// Serial reads of a whole deployed recursive view over a graph of `edges`.
#[derive(Debug, Clone, Serialize)]
pub struct RecursiveParams {
    pub edges: usize,
    pub warmup: usize,
    pub serial: usize,
}

/// Parameters of the engine suite: the cases the gate's fixtures leave out.
#[derive(Debug, Clone, Serialize)]
pub struct EngineParams {
    /// `?two_hop(1, Z)`: a warm non-recursive rule, bound.
    pub rule_query: QueryParams,
    /// `?reach(X, Y)`: the whole transitive closure, unbound.
    pub unbound_query: QueryParams,
    pub writes: WriteParams,
    pub claims: ClaimParams,
    pub why: WhyParams,
    pub sessions: SessionParams,
    pub memory: MemoryParams,
    pub recovery: RecoveryParams,
}

/// Durable retractions and conditional writes against preloaded facts.
#[derive(Debug, Clone, Serialize)]
pub struct WriteParams {
    pub preload: usize,
    /// Serial plain deletes, then as many conditional deletes and updates.
    pub each: usize,
}

/// Guarded inserts (`claim`): wins, losses, and racers on one key.
#[derive(Debug, Clone, Serialize)]
pub struct ClaimParams {
    /// Serial claims of fresh keys (each wins), then of the same keys (each loses).
    pub serial: usize,
    /// Connections racing for each contested key.
    pub racers: usize,
    pub contested_keys: usize,
}

/// `.why` proofs of a non-recursive and a recursive rule.
#[derive(Debug, Clone, Serialize)]
pub struct WhyParams {
    pub nodes: u64,
    pub edges: usize,
    pub warmup: usize,
    pub serial: usize,
}

/// Many sessions, each subscribed to its own bound standing query, in one
/// knowledge graph; one external writer touches one session per write.
#[derive(Debug, Clone, Serialize)]
pub struct SessionParams {
    pub nodes: u64,
    pub edges: usize,
    pub sessions: usize,
    pub writes: usize,
    pub interval_ms: u64,
}

/// Resident memory: idle, per loaded knowledge graph, per base fact.
#[derive(Debug, Clone, Serialize)]
pub struct MemoryParams {
    pub nodes: u64,
    pub edges: usize,
    pub graphs: usize,
    /// Facts loaded into one more graph to measure bytes per fact.
    pub facts: usize,
}

/// Crash (SIGKILL) and restart on the same data directory.
#[derive(Debug, Clone, Serialize)]
pub struct RecoveryParams {
    pub nodes: u64,
    pub edges: usize,
    /// Durable facts written in batches before the first crash.
    pub facts: usize,
    pub batch_size: usize,
    pub restarts: usize,
}

impl EngineParams {
    /// The engine suite at the standard profile's sizes.
    fn standard() -> Self {
        Self {
            rule_query: QueryParams {
                nodes: 2_500,
                edges: 10_000,
                warmup: 20,
                serial: 300,
                clients: 8,
                per_client: 100,
            },
            // A closure of 11,785 rows, under the default 100K result cap.
            unbound_query: QueryParams {
                nodes: 200,
                edges: 300,
                warmup: 3,
                serial: 40,
                clients: 4,
                per_client: 10,
            },
            writes: WriteParams {
                preload: 2_000,
                each: 150,
            },
            claims: ClaimParams {
                serial: 150,
                racers: 8,
                contested_keys: 40,
            },
            why: WhyParams {
                nodes: 200,
                edges: 300,
                warmup: 1,
                serial: 20,
            },
            sessions: SessionParams {
                nodes: 2_500,
                edges: 10_000,
                sessions: 100,
                writes: 100,
                interval_ms: 200,
            },
            memory: MemoryParams {
                nodes: 2_500,
                edges: 10_000,
                graphs: 8,
                // Under the default 100K result cap: the fixture reads them all.
                facts: 90_000,
            },
            recovery: RecoveryParams {
                nodes: 2_500,
                edges: 10_000,
                facts: 50_000,
                batch_size: 1_000,
                restarts: 3,
            },
        }
    }
}

impl Profile {
    pub fn named(name: &str) -> Option<Self> {
        match name {
            "standard" => Some(Self::standard()),
            "quick" => Some(Self::quick()),
            _ => None,
        }
    }

    /// The gate's acceptance profile.
    pub fn standard() -> Self {
        let delta = DeltaParams {
            nodes: 2_500,
            edges: 10_000,
            subscribers: 1,
            writes: 150,
            interval_ms: 20,
        };
        Self {
            name: "standard",
            cheap_query: QueryParams {
                nodes: 2_000,
                edges: 4_000,
                warmup: 50,
                serial: 400,
                clients: 8,
                per_client: 150,
            },
            bound_query: QueryParams {
                nodes: 2_000,
                edges: 4_000,
                warmup: 10,
                serial: 150,
                clients: 8,
                per_client: 25,
            },
            insert: InsertParams {
                single: 200,
                writers: 4,
                per_writer: 50,
                batches: 20,
                batch_size: 1_000,
            },
            delta_single: delta.clone(),
            delta_fanout: DeltaParams {
                subscribers: 64,
                writes: 100,
                interval_ms: 80,
                ..delta.clone()
            },
            delta_first: FirstDeltaParams {
                nodes: delta.nodes,
                edges: delta.edges,
                cycles: 40,
                settle_ms: 100,
            },
            delta_keyed: KeyedParams {
                edges: 100_000,
                keys: 300,
                writes: 120,
                interval_ms: 20,
            },
            interference: InterferenceParams {
                delta: DeltaParams {
                    writes: 120,
                    interval_ms: 30,
                    ..delta
                },
                slow_consumer_requests: 8,
            },
            // The policy's p50 ceiling needs `min_samples_p50` per round.
            shop_install: ShopParams { installs: 20 },
            recursive_query: RecursiveParams {
                edges: 100_000,
                warmup: 3,
                serial: 25,
            },
            engine: EngineParams::standard(),
        }
    }

    /// A smoke-sized profile for developing the gate itself; too small for a
    /// verdict on p99.
    pub fn quick() -> Self {
        let mut profile = Self::standard();
        profile.name = "quick";
        for query in [&mut profile.cheap_query, &mut profile.bound_query] {
            query.warmup = 5;
            query.serial /= 5;
            query.per_client /= 5;
        }
        profile.insert.single /= 5;
        profile.insert.per_writer /= 5;
        profile.insert.batches /= 5;
        profile.delta_single.writes /= 5;
        profile.delta_fanout.writes /= 5;
        profile.delta_fanout.subscribers /= 4;
        profile.delta_first.cycles /= 4;
        profile.delta_keyed.keys /= 4;
        profile.delta_keyed.writes /= 4;
        profile.interference.delta.writes /= 5;
        profile.shop_install.installs /= 4;
        profile.recursive_query.warmup = 1;
        profile.recursive_query.serial /= 5;
        let engine = &mut profile.engine;
        for query in [&mut engine.rule_query, &mut engine.unbound_query] {
            query.warmup = 1;
            query.serial /= 5;
            query.per_client /= 5;
        }
        engine.writes.each /= 5;
        engine.claims.serial /= 5;
        engine.claims.contested_keys /= 4;
        engine.why.serial /= 4;
        engine.sessions.sessions /= 4;
        engine.sessions.writes /= 4;
        engine.memory.graphs /= 2;
        engine.memory.facts /= 10;
        engine.recovery.facts /= 5;
        engine.recovery.restarts = 1;
        profile
    }
}
