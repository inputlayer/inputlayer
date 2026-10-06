//! S10 and S11, vector indexes on tables and views at the same revision.
//!
//! S10: an embedding change updates a live similarity rule. Captain's table
//! row 8 (vector indexes on tables and views at one revision, similarity
//! inside live rules). Gates VV2 #324, VV5 #327 and VV6 #328.
//!
//! On the vector shop pack (`emb_idx` over `embedding`, the deployed rule
//! `near(Item, Other, D)`: an item's five nearest neighbours) an agent
//! subscribes to item 1's neighbours. A writer moves item 1's embedding (one
//! delta replaces the four neighbours besides item 1 itself), inserts a candidate closer than every
//! neighbour but item 1 itself (one row in, the displaced fifth out) and
//! deletes it again (the fifth comes back). Required today: every delta is
//! exact against a fresh query on another connection and names the write's
//! revision, every answer equals a brute-force cosine search over the
//! embeddings, and a second run of the same history on a fresh engine
//! delivers identical deltas. VV2 keeps this half green when `emb_idx` is
//! maintained inside the view maintainer (`maintained` mode).
//!
//! Expected failures: reading `near` evaluates the rule (the counters do not
//! exist until V1 #308; then the keyed top-k operator, VV5 #327, serves the
//! read from the view), and a read `at` an earlier revision answers at that
//! revision (query replies carry no `revision` until V9 #315; then vector
//! reads at a stated revision are VV6 #328).
//!
//! S11: an index on a view equals the view at every revision. Row 8 again;
//! gates VV1 #323 (vector columns in maintained views), VV3 #325 and VV6
//! #328. A deployed rule `low_vec` carries the embeddings of items below 50,
//! and `.index create view_idx on low_vec(...)` indexes it. A base write that
//! drops a row of the view, a rule replacement (one program: `.rule clear`
//! and the new clause) and a restart follow. Required today: the view's rows
//! are right at each step. Expected failure (VV3 #325): the index's rows are
//! the view's rows at every step, and its definition survives the restart;
//! today the index is not built over the view's rows.

use std::collections::BTreeSet;

use inputlayer_testkit::agent::row_keys;
use inputlayer_testkit::{
    Agent, Checked, Counters, Delta, Engine, Fixture, KnownDefect, Reproduction, Size, Violation,
    WsClient,
};
use serde_json::Value;

use crate::consistency::NO_REVISION;
use crate::engine;
use crate::lifecycle::KG;
use crate::support::{
    committed, fresh, no_rule_evaluations, revision_aligned, write_revision_matches_delta,
    NO_VIEW_COUNTERS,
};

/// The agent's subscription: item 1's five nearest neighbours.
const NEAR: &str = "?near(1, Other, D)";
/// Neighbours `near` keeps per item.
const K: usize = 5;
/// Items of the vector shop pack (`embedding` keys 0..ITEMS).
const ITEMS: i64 = 100;
/// Dimensions of a shop pack embedding.
const DIMENSIONS: usize = 8;
/// A cosine distance the engine reports and one computed here agree to this.
const EPSILON: f64 = 1e-5;

/// Item 1's new embedding, and a candidate closer to it than any other item.
const MOVED: [f32; DIMENSIONS] = [0.9, 0.1, 0.8, 0.2, 0.7, 0.3, 0.6, 0.4];
const CANDIDATE: i64 = 1000;
const CLOSER: [f32; DIMENSIONS] = [0.9, 0.1, 0.8, 0.2, 0.7, 0.3, 0.6, 0.41];

/// Reading `near` evaluates the rule instead of its view (VV5 #327).
const NEAR_EVALUATED: KnownDefect = KnownDefect {
    plan_item: "#327",
    summary: "a read of the deployed similarity rule re-derives it instead of reading its view",
    signature: |v| matches!(v, Violation::UnexpectedWork(_)),
    reproduction: Reproduction::Deterministic,
};

/// A vector read `at` an earlier revision answers at the latest (VV6 #328).
const VECTOR_AT: KnownDefect = KnownDefect {
    plan_item: "#328",
    summary: "a similarity read `at: r` answers at the latest revision instead of r",
    signature: |v| {
        matches!(
            v,
            Violation::StaleRevision { .. } | Violation::Diverged { .. }
        )
    },
    reproduction: Reproduction::Deterministic,
};

/// An index on a view does not hold the view's rows (VV3 #325).
const INDEX_ON_VIEW: KnownDefect = KnownDefect {
    plan_item: "#325",
    summary: "an index created on a deployed rule does not hold the view's rows",
    signature: |v| {
        matches!(
            v,
            Violation::Rejected(_) | Violation::Diverged { .. } | Violation::Transport(_)
        )
    },
    reproduction: Reproduction::Deterministic,
};

/// Component `d` of item `n`'s embedding in the vector shop pack, in
/// hundredths (`Fixture::shop_pack` writes it as `0.{:02}`).
fn pack_hundredths(n: i64, d: usize) -> usize {
    (usize::try_from(n).expect("pack items are non-negative") * 31 + d * 17) % 100
}

/// The vector shop pack's embedding of item `n`.
fn pack_embedding(n: i64) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|d| {
            let hundredths = u8::try_from(pack_hundredths(n, d)).expect("below 100");
            f32::from(hundredths) / 100.0
        })
        .collect()
}

fn literal(vector: &[f32]) -> String {
    let parts: Vec<String> = vector.iter().map(|x| format!("{x}")).collect();
    format!("[{}]", parts.join(", "))
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f64 {
    let dot: f64 = a
        .iter()
        .zip(b)
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum();
    let norm = |v: &[f32]| v.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
    1.0 - dot / (norm(a) * norm(b))
}

/// The embeddings a history has written so far, by item.
struct Embeddings(Vec<(i64, Vec<f32>)>);

impl Embeddings {
    fn pack() -> Self {
        Self((0..ITEMS).map(|n| (n, pack_embedding(n))).collect())
    }

    fn set(&mut self, item: i64, vector: &[f32]) {
        self.0.retain(|(n, _)| *n != item);
        self.0.push((item, vector.to_vec()));
    }

    fn remove(&mut self, item: i64) {
        self.0.retain(|(n, _)| *n != item);
    }

    /// Brute force: `item`'s `K` nearest neighbours (itself included) by
    /// cosine distance, nearest first.
    fn nearest(&self, item: i64) -> Vec<(i64, f64)> {
        let (_, probe) = self.0.iter().find(|(n, _)| *n == item).expect("item");
        let mut all: Vec<(i64, f64)> = self
            .0
            .iter()
            .map(|(n, v)| (*n, cosine_distance(probe, v)))
            .collect();
        all.sort_by(|a, b| a.1.total_cmp(&b.1));
        all.truncate(K);
        all
    }
}

/// Fail unless `rows` (`[1, Other, D]`) are exactly the brute-force answer.
fn assert_brute_force(rows: &[Value], expected: &[(i64, f64)]) -> Checked<()> {
    let mut got: Vec<(i64, f64)> = rows
        .iter()
        .map(|row| {
            (
                row[1].as_i64().unwrap_or(-1),
                row[2].as_f64().unwrap_or(f64::NAN),
            )
        })
        .collect();
    got.sort_by(|a, b| a.1.total_cmp(&b.1));
    let same = got.len() == expected.len()
        && got
            .iter()
            .zip(expected)
            .all(|(g, e)| g.0 == e.0 && (g.1 - e.1).abs() < EPSILON);
    if same {
        return Ok(());
    }
    Err(Violation::Diverged {
        subscription: "near(1, ..) against a brute-force search".into(),
        missing: expected.iter().map(|e| format!("{e:?}")).collect(),
        unexpected: got.iter().map(|g| format!("{g:?}")).collect(),
    })
}

/// One write of S10, its brute-force effect and the shape of its delta.
struct Step {
    program: String,
    apply: fn(&mut Embeddings),
    /// Rows the delta inserts and retracts.
    shape: (usize, usize),
}

fn s10_steps() -> Vec<Step> {
    vec![
        Step {
            program: format!(
                "-embedding(X, V) <- embedding(X, V), X = 1\n+embedding(1, {})",
                literal(&MOVED)
            ),
            apply: |e| e.set(1, &MOVED),
            // Item 1 stays its own nearest neighbour, at distance 0.
            shape: (K - 1, K - 1),
        },
        Step {
            program: format!("+embedding({CANDIDATE}, {})", literal(&CLOSER)),
            apply: |e| e.set(CANDIDATE, &CLOSER),
            shape: (1, 1),
        },
        Step {
            program: format!("-embedding(X, V) <- embedding(X, V), X = {CANDIDATE}"),
            apply: |e| e.remove(CANDIDATE),
            shape: (1, 1),
        },
    ]
}

/// What one S10 run observed: the snapshot, then each delta's rows and
/// revision.
#[derive(Debug, PartialEq)]
struct Run {
    snapshot: BTreeSet<String>,
    deltas: Vec<(BTreeSet<String>, BTreeSet<String>, u64)>,
}

fn row_set(rows: &[Value]) -> BTreeSet<String> {
    row_keys(rows)
}

fn keys_to_rows(keys: &BTreeSet<String>) -> Vec<Value> {
    keys.iter()
        .map(|k| serde_json::from_str(k).expect("row key is JSON"))
        .collect()
}

/// Run S10's required half on a fresh engine; returns what the agent saw.
/// With `expected_failures`, also judge the counter and `at` parts.
async fn s10_run(expected_failures: bool) -> Checked<Run> {
    let engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Vector).install(&engine).await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;
    let mut embeddings = Embeddings::pack();

    let snapshot = agent.subscribe("near", NEAR).await?.rows.clone();
    assert_brute_force(&keys_to_rows(&snapshot), &embeddings.nearest(1))?;
    agent
        .view("near")
        .assert_matches(&fresh(&mut auditor, NEAR).await?)?;

    let mut run = Run {
        snapshot,
        deltas: Vec::new(),
    };
    let mut history: Vec<(u64, BTreeSet<String>)> = Vec::new();
    for step in s10_steps() {
        let before = agent.view("near").rows.clone();
        let write = committed(writer.try_execute(&step.program).await?)?;
        (step.apply)(&mut embeddings);
        let delta: Delta = agent.next_delta("near").await?;
        write_revision_matches_delta(&write, &delta)?;
        let now = fresh(&mut auditor, NEAR).await?;
        agent.view("near").assert_matches(&now)?;
        assert_brute_force(&now, &embeddings.nearest(1))?;
        let after = row_set(&now);
        delta.assert_rows(
            &keys_to_rows(&after.difference(&before).cloned().collect()),
            &keys_to_rows(&before.difference(&after).cloned().collect()),
        )?;
        assert_eq!(
            (delta.inserted.len(), delta.retracted.len()),
            step.shape,
            "rows in and out of near(1, ..) for `{}`",
            step.program
        );
        history.push((delta.revision, after));
        run.deltas
            .push((delta.inserted, delta.retracted, delta.revision));
    }

    if expected_failures {
        let before = engine.metrics().await.expect("scrape metrics");
        auditor.query(NEAR).await?;
        let after = engine.metrics().await.expect("scrape metrics");
        KnownDefect::judge_first(
            &[NO_VIEW_COUNTERS, NEAR_EVALUATED],
            no_rule_evaluations(&Counters::delta(&before, &after), 1),
        );

        // The candidate's revision, read after it was deleted again.
        let (revision, rows) = &history[1];
        let read = auditor.execute_at(NEAR, *revision).await?;
        KnownDefect::judge_first(
            &[NO_REVISION, VECTOR_AT],
            revision_aligned(&read, *revision, Value::clone, rows),
        );
    }
    Ok(run)
}

#[tokio::test(flavor = "multi_thread")]
async fn s10_embedding_change_updates_a_live_similarity_rule() -> Checked<()> {
    let first = s10_run(true).await?;
    let second = s10_run(false).await?;
    assert_eq!(
        first, second,
        "the same history on a fresh engine delivers the same snapshot, deltas and revisions"
    );
    Ok(())
}

/// The view S11 indexes: embeddings of items below `bound`.
fn low_vec(bound: i64) -> String {
    format!("low_vec(Item, V) <- embedding(Item, V), Item < {bound}")
}

/// A probe for `view_idx` asking for more neighbours than the view has rows,
/// so the answer is every indexed row.
const ALL_OF_VIEW: &str = "?hnsw_nearest(\"view_idx\", [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5], \
                           100, Id, D)";

/// The items `?low_vec(I, V)` answers.
async fn view_items(auditor: &mut WsClient) -> Checked<BTreeSet<i64>> {
    Ok(auditor
        .query("?low_vec(I, V)")
        .await?
        .rows
        .iter()
        .filter_map(|row| row[0].as_i64())
        .collect())
}

/// `view_idx` holds exactly the view's items.
async fn index_matches_view(auditor: &mut WsClient, items: &BTreeSet<i64>) -> Checked<()> {
    let reply = auditor.try_execute(ALL_OF_VIEW).await?;
    let rows = match reply {
        Ok(result) if result.errors.is_empty() => result.rows,
        Ok(result) => return Err(Violation::Rejected(format!("{:?}", result.errors))),
        Err(refusal) => return Err(Violation::Rejected(refusal.message)),
    };
    let indexed: BTreeSet<i64> = rows.iter().filter_map(|row| row[0].as_i64()).collect();
    if &indexed == items {
        return Ok(());
    }
    Err(Violation::Diverged {
        subscription: "view_idx against low_vec".into(),
        missing: items
            .difference(&indexed)
            .map(ToString::to_string)
            .collect(),
        unexpected: indexed.difference(items).map(ToString::to_string).collect(),
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn s11_index_on_a_view_follows_the_view() -> Checked<()> {
    let mut engine: Engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Vector).install(&engine).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;
    committed(writer.try_execute(&format!("+{}", low_vec(50))).await?)?;
    assert_eq!(view_items(&mut auditor).await?, (0..50).collect());

    // The index's checks accumulate; the first violation is the outcome.
    let created = writer
        .try_execute(".index create view_idx on low_vec(V) metric cosine")
        .await?;
    let mut index: Checked<()> = committed(created).map(drop);
    if index.is_ok() {
        index = index_matches_view(&mut auditor, &(0..50).collect()).await;
    }

    let steps: [(String, BTreeSet<i64>); 2] = [
        (
            "-embedding(X, V) <- embedding(X, V), X = 7".to_string(),
            (0..50).filter(|n| *n != 7).collect(),
        ),
        (
            format!(".rule clear low_vec\n+{}", low_vec(30)),
            (0..30).filter(|n| *n != 7).collect(),
        ),
    ];
    for (program, expected) in &steps {
        committed(writer.try_execute(program).await?)?;
        assert_eq!(
            &view_items(&mut auditor).await?,
            expected,
            "after `{program}`"
        );
        if index.is_ok() {
            index = index_matches_view(&mut auditor, expected).await;
        }
    }

    writer.close().await;
    auditor.close().await;
    engine.restart().await.expect("restart engine");
    let mut auditor = WsClient::connect(&engine, KG).await?;
    let expected = &steps[1].1;
    assert_eq!(
        &view_items(&mut auditor).await?,
        expected,
        "after the restart"
    );
    if index.is_ok() {
        index = index_matches_view(&mut auditor, expected).await;
    }
    INDEX_ON_VIEW.judge(index);
    Ok(())
}

#[test]
fn pack_embeddings_follow_the_fixture() {
    // `Fixture::shop_pack(Size::Vector)` writes each component as `0.{:02}`.
    let fixture = Fixture::shop_pack(Size::Vector);
    let facts = fixture
        .statements
        .iter()
        .find(|s| s.starts_with("+embedding["))
        .expect("embedding facts");
    for n in [0, 1, 42, 99] {
        let parts: Vec<String> = (0..DIMENSIONS)
            .map(|d| format!("0.{:02}", pack_hundredths(n, d)))
            .collect();
        let tuple = format!("({n}, [{}])", parts.join(", "));
        assert!(facts.contains(&tuple), "{tuple} in the fixture");
    }
    let near = Embeddings::pack().nearest(1);
    assert_eq!(near[0].0, 1, "an item is its own nearest neighbour");
    assert!(near.windows(2).all(|w| w[0].1 <= w[1].1));
}
