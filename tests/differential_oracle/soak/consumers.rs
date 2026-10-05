//! The soak's consumers: subscribers of every kind, and auditors.
//!
//! Every consumer reports each result it holds, with the revision the engine
//! says it is exact at, to the verifier. A consumer that keeps up must never
//! be disconnected. One that reads slowly or stops reading may be, but only
//! as the protocol documents (a `slow_consumer` notice, or the send timeout
//! closing the socket); it then reconnects and subscribes again.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::{Duration, Instant};

use inputlayer_testkit::client::{Frame, Pacing};
use inputlayer_testkit::{Agent, Checked, Violation, WsClient};
use serde_json::Value;

use super::verify::{Class, Digest, Event, Kind, Seen};
use super::{Config, Shared};
use crate::generate::Rng;

/// How long one wait for a push lasts before the loop looks around.
const POLL: Duration = Duration::from_millis(100);
/// Keep-alive interval: the engine closes connections idle for 5 minutes,
/// and pushes alone do not count as activity.
const PING_EVERY: Duration = Duration::from_secs(60);
/// The socket receive buffer of slow and stalled consumers.
const SMALL_RECV_BUFFER: u32 = 4096;

/// What one consumer saw over the run.
#[derive(Debug, Default, Clone)]
pub struct ConsumerStats {
    pub connections: u64,
    pub deltas: u64,
    pub stalls: u64,
    /// Disconnects the protocol allows, by reason.
    pub disconnects: BTreeMap<String, u64>,
    /// `notifications_missed` notices.
    pub notifications_missed: u64,
}

impl ConsumerStats {
    pub fn merge(&mut self, other: &Self) {
        self.connections += other.connections;
        self.deltas += other.deltas;
        self.stalls += other.stalls;
        self.notifications_missed += other.notifications_missed;
        for (reason, n) in &other.disconnects {
            *self.disconnects.entry(reason.clone()).or_default() += n;
        }
    }
}

/// What every consumer needs.
#[derive(Clone)]
pub struct Ctx {
    pub config: Arc<Config>,
    pub shared: Arc<Shared>,
    pub queries: Arc<Vec<String>>,
    /// The first `core` queries are the shared standing queries; the rest
    /// are bound ones (`?reach(k, Y)`), one per subscriber.
    pub core: usize,
    pub events: Sender<Event>,
}

impl Ctx {
    fn seen(
        &self,
        who: &Arc<str>,
        class: Class,
        query: usize,
        revision: u64,
        digest: Digest,
        kind: Kind,
        at: Instant,
    ) {
        let _ = self.events.send(Event::Seen(Seen {
            who: Arc::clone(who),
            class,
            query,
            revision,
            digest,
            kind,
            at,
        }));
    }

    /// The core queries plus one bound query chosen by `index`.
    fn plan(&self, index: usize) -> Vec<usize> {
        let mut plan: Vec<usize> = (0..self.core).collect();
        let bound = self.queries.len() - self.core;
        if bound > 0 {
            plan.push(self.core + index % bound);
        }
        plan
    }
}

/// Why a connection ended, when the protocol allows it to end that way: the
/// engine's `slow_consumer` notice, or the socket closing under a consumer
/// that read too slowly (the send timeout).
fn allowed_disconnect(violation: &Violation) -> Option<&'static str> {
    match violation {
        Violation::Transport(message) if message.contains("\"slow_consumer\"") => {
            Some("slow_consumer")
        }
        Violation::Transport(message)
            if message.contains("connection closed") || message.contains("send:") =>
        {
            Some("closed")
        }
        _ => None,
    }
}

/// One subscribed connection and the digests of its results.
struct Session {
    agent: Agent,
    /// Subscription id -> (query index, digest of the maintained rows).
    digests: BTreeMap<String, (usize, Digest)>,
    last_ping: Instant,
}

impl Session {
    async fn open(ctx: &Ctx, who: &Arc<str>, class: Class, plan: &[usize]) -> Checked<Self> {
        let pacing = match class {
            Class::Slow | Class::Stalled => Pacing {
                inbox_frames: 1,
                recv_buffer_bytes: Some(SMALL_RECV_BUFFER),
            },
            _ => Pacing::EAGER,
        };
        let client =
            WsClient::connect_paced(&ctx.shared.ws_url, &ctx.shared.api_key, pacing).await?;
        let mut agent = Agent::over(client);
        let mut digests = BTreeMap::new();
        for &query in plan {
            let id = format!("q{query}");
            let view = agent.subscribe(&id, &ctx.queries[query]).await?;
            let digest = Digest::of(&view.rows);
            ctx.seen(
                who,
                class,
                query,
                view.revision,
                digest,
                Kind::Snapshot,
                Instant::now(),
            );
            digests.insert(id, (query, digest));
        }
        Ok(Self {
            agent,
            digests,
            last_ping: Instant::now(),
        })
    }

    /// Apply the next delta, if one arrives within [`POLL`].
    async fn step(&mut self, ctx: &Ctx, who: &Arc<str>, class: Class) -> Checked<bool> {
        if self.last_ping.elapsed() >= PING_EVERY {
            self.agent.client_mut().ping().await?;
            self.last_ping = Instant::now();
        }
        self.agent.clear_notices();
        let Some(delta) = self.agent.next_any_delta(POLL).await? else {
            return Ok(false);
        };
        let Some((query, digest)) = self.digests.get_mut(&delta.subscription) else {
            return Err(Violation::UnexpectedPush(format!(
                "a delta for unknown subscription '{}'",
                delta.subscription
            )));
        };
        // The agent checked that retracted rows were present and inserted
        // ones absent, so the digest follows the view exactly.
        for row in &delta.retracted {
            digest.retract(row);
        }
        for row in &delta.inserted {
            digest.insert(row);
        }
        ctx.seen(
            who,
            class,
            *query,
            delta.revision,
            *digest,
            Kind::Delta,
            delta.at,
        );
        Ok(true)
    }

    /// Claim every result exact at the final revision.
    fn claim_final(&self, ctx: &Ctx, who: &Arc<str>, class: Class, at: u64) -> Checked<()> {
        for (id, (query, digest)) in &self.digests {
            let view = self.agent.view(id);
            if view.revision > at {
                return Err(Violation::StaleRevision {
                    subscription: id.clone(),
                    previous: at,
                    got: view.revision,
                });
            }
            ctx.seen(who, class, *query, at, *digest, Kind::Final, Instant::now());
        }
        Ok(())
    }

    fn notifications_missed(&self) -> u64 {
        self.agent
            .client()
            .notices()
            .iter()
            .filter(|n| n.value["code"] == "notifications_missed")
            .count() as u64
    }
}

/// Sleep `total`, waking early when the run settles or aborts.
async fn pause(shared: &Shared, total: Duration) {
    let until = Instant::now() + total;
    while Instant::now() < until && shared.final_revision().is_none() && !shared.aborted() {
        tokio::time::sleep(POLL.min(until.saturating_duration_since(Instant::now()))).await;
    }
}

/// A subscriber of class `class` (fast, slow, stalled or churn).
pub async fn subscriber(class: Class, index: usize, ctx: Ctx) -> ConsumerStats {
    let who: Arc<str> = format!("{}-{index}", class.name()).into();
    let mut stats = ConsumerStats::default();
    if let Err(message) = subscribe_loop(class, index, &ctx, &who, &mut stats).await {
        ctx.shared.fail(format!("{who}: {message}"));
    }
    stats
}

async fn subscribe_loop(
    class: Class,
    index: usize,
    ctx: &Ctx,
    who: &Arc<str>,
    stats: &mut ConsumerStats,
) -> Result<(), String> {
    let config = &ctx.config;
    let mut rng = Rng::new(config.seed ^ ((class as u64) << 40) ^ index as u64);
    let quiet = Duration::from_millis(config.quiet_ms);
    let stall = Duration::from_millis(config.stall_ms);
    let mut session: Option<Session> = None;
    let mut quiet_since = Instant::now();
    let mut next_stall = Instant::now() + stall.mul_f64(1.0 + rng.below(100) as f64 / 50.0);
    let mut leave_at = Instant::now();
    loop {
        if ctx.shared.aborted() {
            return Ok(());
        }
        let settling = ctx.shared.final_revision();
        let mut current = match session.take() {
            Some(current) => current,
            None => {
                let plan = if class == Class::Churn {
                    // A few shared queries and a bound one, for a short while.
                    let mut plan: Vec<usize> = (0..3).map(|_| rng.below(ctx.core)).collect();
                    plan.sort_unstable();
                    plan.dedup();
                    plan.extend(ctx.plan(index + stats.connections as usize).last());
                    plan
                } else {
                    ctx.plan(index)
                };
                let opened = Session::open(ctx, who, class, &plan).await;
                match opened {
                    Ok(opened) => {
                        stats.connections += 1;
                        quiet_since = Instant::now();
                        leave_at =
                            Instant::now() + Duration::from_millis(500 + rng.below(2500) as u64);
                        opened
                    }
                    Err(e) => return Err(format!("subscribe: {e}")),
                }
            }
        };
        if class == Class::Stalled && settling.is_none() && Instant::now() >= next_stall {
            // Stop reading: the inbox fills, then the socket, then the
            // engine's send to this connection blocks.
            stats.stalls += 1;
            pause(&ctx.shared, stall).await;
            next_stall = Instant::now() + stall.mul_f64(1.0 + rng.below(100) as f64 / 50.0);
        }
        if class == Class::Churn && settling.is_none() && Instant::now() >= leave_at {
            stats.notifications_missed += current.notifications_missed();
            current.agent.disconnect().await;
            continue;
        }
        match current.step(ctx, who, class).await {
            Ok(true) => {
                stats.deltas += 1;
                quiet_since = Instant::now();
                if class == Class::Slow {
                    let ms = rng.below(2 * config.slow_pause_ms as usize + 1) as u64;
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                }
                session = Some(current);
            }
            Ok(false) => {
                if let Some(at) = settling {
                    if quiet_since.elapsed() >= quiet {
                        current
                            .claim_final(ctx, who, class, at)
                            .map_err(|e| e.to_string())?;
                        stats.notifications_missed += current.notifications_missed();
                        current.agent.disconnect().await;
                        return Ok(());
                    }
                }
                session = Some(current);
            }
            Err(violation) => {
                stats.notifications_missed += current.notifications_missed();
                match allowed_disconnect(&violation) {
                    Some(reason) if matches!(class, Class::Slow | Class::Stalled) => {
                        *stats.disconnects.entry(reason.to_string()).or_default() += 1;
                        // Reconnect and subscribe again on the next turn.
                    }
                    _ => return Err(violation.to_string()),
                }
            }
        }
    }
}

/// A group subscriber: every core query in one subscription group.
pub async fn group(index: usize, ctx: Ctx) -> ConsumerStats {
    let who: Arc<str> = format!("group-{index}").into();
    let mut stats = ConsumerStats::default();
    if let Err(message) = group_loop(&ctx, &who, &mut stats).await {
        ctx.shared.fail(format!("{who}: {message}"));
    }
    stats
}

/// One member's maintained rows.
struct Member {
    rows: BTreeSet<String>,
    digest: Digest,
}

async fn group_loop(ctx: &Ctx, who: &Arc<str>, stats: &mut ConsumerStats) -> Result<(), String> {
    let names: Vec<String> = (0..ctx.core).map(|i| format!("m{i}")).collect();
    let named: Vec<(&str, &str)> = names
        .iter()
        .zip(ctx.queries.iter())
        .map(|(n, q)| (n.as_str(), q.as_str()))
        .collect();
    let mut client = WsClient::connect_url(&ctx.shared.ws_url, &ctx.shared.api_key)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    stats.connections += 1;
    let snapshot = client
        .subscribe_group("g", &named)
        .await
        .map_err(|e| format!("subscribe: {e}"))?;
    let mut members: Vec<Member> = Vec::new();
    for (query, (_, rows)) in snapshot.results.iter().enumerate() {
        let rows: BTreeSet<String> = rows.iter().map(Value::to_string).collect();
        let digest = Digest::of(&rows);
        ctx.seen(
            who,
            Class::Group,
            query,
            snapshot.revision,
            digest,
            Kind::Snapshot,
            snapshot.at,
        );
        members.push(Member { rows, digest });
    }
    if members.len() != names.len() {
        return Err(format!(
            "snapshot has {} results for {} members",
            members.len(),
            names.len()
        ));
    }
    let (mut seq, mut revision) = (0u64, snapshot.revision);
    let quiet = Duration::from_millis(ctx.config.quiet_ms);
    let mut quiet_since = Instant::now();
    let mut last_ping = Instant::now();
    loop {
        if ctx.shared.aborted() {
            return Ok(());
        }
        if last_ping.elapsed() >= PING_EVERY {
            client.ping().await.map_err(|e| format!("ping: {e}"))?;
            last_ping = Instant::now();
        }
        let frame = client.poll_push(POLL).await.map_err(|e| e.to_string())?;
        let Some(frame) = frame else {
            if let Some(at) = ctx.shared.final_revision() {
                if quiet_since.elapsed() >= quiet {
                    if revision > at {
                        return Err(format!("group at revision {revision} past the final {at}"));
                    }
                    for (query, member) in members.iter().enumerate() {
                        ctx.seen(
                            who,
                            Class::Group,
                            query,
                            at,
                            member.digest,
                            Kind::Final,
                            Instant::now(),
                        );
                    }
                    client.close().await;
                    return Ok(());
                }
            }
            continue;
        };
        match frame.kind() {
            "subscription_group_delta" => {
                apply_group_delta(&frame, &names, &mut members, &mut seq, &mut revision)?;
                stats.deltas += 1;
                quiet_since = Instant::now();
                for (query, member) in members.iter().enumerate() {
                    ctx.seen(
                        who,
                        Class::Group,
                        query,
                        revision,
                        member.digest,
                        Kind::Delta,
                        frame.at,
                    );
                }
            }
            kind if kind.starts_with("subscription_") => {
                return Err(format!("unexpected group push: {}", frame.value));
            }
            // Change notifications.
            _ => {}
        }
    }
}

fn apply_group_delta(
    frame: &Frame,
    names: &[String],
    members: &mut [Member],
    seq: &mut u64,
    revision: &mut u64,
) -> Result<(), String> {
    let value = &frame.value;
    let got_seq = value["seq"].as_u64().unwrap_or_default();
    let got_revision = value["revision"].as_u64().unwrap_or_default();
    if value["subscription"] != "g" || got_seq != *seq + 1 {
        return Err(format!(
            "group delta out of sequence after seq {seq}: {value}"
        ));
    }
    if got_revision <= *revision {
        return Err(format!(
            "group delta at revision {got_revision} after one at {revision}"
        ));
    }
    let deltas = value["members"].as_array().cloned().unwrap_or_default();
    if deltas.len() != members.len() {
        return Err(format!(
            "group delta lists {} members: {value}",
            deltas.len()
        ));
    }
    for ((delta, member), name) in deltas.iter().zip(members.iter_mut()).zip(names) {
        if delta["name"] != name.as_str() {
            return Err(format!("group delta out of member order: {value}"));
        }
        let rows = |field: &str| -> Vec<String> {
            delta[field]
                .as_array()
                .map(|rows| rows.iter().map(Value::to_string).collect())
                .unwrap_or_default()
        };
        let (inserted, retracted) = (rows("inserted"), rows("retracted"));
        if delta["unchanged"] == true && !(inserted.is_empty() && retracted.is_empty()) {
            return Err(format!(
                "member {name} is unchanged but carries rows: {value}"
            ));
        }
        for row in &retracted {
            if !member.rows.remove(row) {
                return Err(format!("member {name} retracted absent row {row}"));
            }
            member.digest.retract(row);
        }
        for row in &inserted {
            if !member.rows.insert(row.clone()) {
                return Err(format!("member {name} inserted present row {row}"));
            }
            member.digest.insert(row);
        }
    }
    *seq = got_seq;
    *revision = got_revision;
    Ok(())
}

/// An auditor: reads every query at one revision, over and over.
pub async fn auditor(index: usize, ctx: Ctx) -> ConsumerStats {
    let who: Arc<str> = format!("read-{index}").into();
    let mut stats = ConsumerStats::default();
    if let Err(message) = audit_loop(&ctx, &who, &mut stats).await {
        ctx.shared.fail(format!("{who}: {message}"));
    }
    stats
}

async fn audit_loop(ctx: &Ctx, who: &Arc<str>, stats: &mut ConsumerStats) -> Result<(), String> {
    let names: Vec<String> = (0..ctx.queries.len()).map(|i| format!("r{i}")).collect();
    let named: Vec<(&str, &str)> = names
        .iter()
        .zip(ctx.queries.iter())
        .map(|(n, q)| (n.as_str(), q.as_str()))
        .collect();
    let mut client = WsClient::connect_url(&ctx.shared.ws_url, &ctx.shared.api_key)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    stats.connections += 1;
    let mut last = 0u64;
    loop {
        if ctx.shared.aborted() {
            return Ok(());
        }
        // Read once more after the writers stopped: that read is final.
        let settled = ctx.shared.final_revision();
        let snapshot = client
            .read(&named)
            .await
            .map_err(|e| format!("read: {e}"))?;
        if snapshot.revision < last {
            return Err(format!(
                "read at revision {} after one at {last} on the same connection",
                snapshot.revision
            ));
        }
        last = snapshot.revision;
        stats.deltas += 1;
        for (query, (_, rows)) in snapshot.results.iter().enumerate() {
            let keys: Vec<String> = rows.iter().map(Value::to_string).collect();
            ctx.seen(
                who,
                Class::Read,
                query,
                snapshot.revision,
                Digest::of(&keys),
                Kind::Read,
                snapshot.at,
            );
        }
        if let Some(at) = settled {
            if snapshot.revision < at {
                return Err(format!(
                    "read after the writers stopped is at revision {}, before the final {at}",
                    snapshot.revision
                ));
            }
            client.close().await;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(ctx.config.audit_ms)).await;
    }
}
