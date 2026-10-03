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
    pub interference: InterferenceParams,
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

/// One probe subscriber while a long request loops on another connection and
/// a slow consumer stops reading its socket.
#[derive(Debug, Clone, Serialize)]
pub struct InterferenceParams {
    pub delta: DeltaParams,
    /// Large results the slow consumer requests and never reads.
    pub slow_consumer_requests: usize,
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
            interference: InterferenceParams {
                delta: DeltaParams {
                    writes: 120,
                    interval_ms: 30,
                    ..delta
                },
                slow_consumer_requests: 8,
            },
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
        profile.interference.delta.writes /= 5;
        profile
    }
}
