//! HNSW vector index on the server path: `.index` commands through `Handler`,
//! incremental maintenance, rebuild, restart, and `hnsw_nearest` queries.

// Test setup aborts on failure; `unwrap` is the intended behavior.
#![allow(clippy::unwrap_used)]

use inputlayer::protocol::wire::{QueryResult, WireValue};
use inputlayer::protocol::Handler;
use inputlayer::{Config, StorageEngine};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::path::Path;
use tempfile::TempDir;

fn handler_at(dir: &Path) -> Handler {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    Handler::new(StorageEngine::new(config).expect("storage engine"))
}

async fn run(handler: &Handler, program: &str) -> Result<QueryResult, String> {
    handler.query_program(None, program.to_string()).await
}

/// The message of the program's failed statement.
async fn failure(handler: &Handler, program: &str) -> String {
    match run(handler, program).await {
        Err(e) => e,
        Ok(result) => result
            .errors
            .first()
            .unwrap_or_else(|| panic!("'{program}' succeeded"))
            .message
            .clone(),
    }
}

async fn exec(handler: &Handler, program: &str) -> QueryResult {
    run(handler, program)
        .await
        .unwrap_or_else(|e| panic!("'{program}' failed: {e}"))
}

/// All string cells of a result (meta commands report through these).
fn text(result: &QueryResult) -> String {
    result
        .rows
        .iter()
        .flat_map(|r| r.values.iter())
        .filter_map(|v| match v {
            WireValue::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Integer values of the named result column, in result order.
fn int_column(result: &QueryResult, column: &str) -> Vec<i64> {
    let idx = result
        .schema
        .iter()
        .position(|c| c.name == column)
        .unwrap_or_else(|| panic!("no column {column} in {:?}", result.schema));
    result
        .rows
        .iter()
        .map(|r| match &r.values[idx] {
            WireValue::Int64(v) => *v,
            WireValue::Int32(v) => i64::from(*v),
            other => panic!("expected int, got {other:?}"),
        })
        .collect()
}

fn sorted(mut v: Vec<i64>) -> Vec<i64> {
    v.sort_unstable();
    v
}

fn vec_literal(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| format!("{x:?}")).collect();
    format!("[{}]", parts.join(", "))
}

const SETUP: &str = r#"
+docs(id: int, title: string, emb: vector)
+docs[(1, "x", [1.0, 0.0, 0.0]), (2, "y", [0.0, 1.0, 0.0]), (3, "z", [0.0, 0.0, 1.0]), (4, "xy", [0.7, 0.7, 0.0])]
"#;

async fn setup(handler: &Handler) {
    for line in SETUP.lines().map(str::trim).filter(|l| !l.is_empty()) {
        exec(handler, line).await;
    }
    let created = exec(
        handler,
        ".index create doc_idx on docs(emb) metric cosine m 16 ef_search 50",
    )
    .await;
    assert!(
        text(&created).contains("Index 'doc_idx' created on docs.emb (4 vectors)"),
        "{}",
        text(&created)
    );
}

async fn nearest(handler: &Handler, query: &str, k: usize) -> Vec<i64> {
    let q = format!(r#"?hnsw_nearest("doc_idx", {query}, {k}, Id, Dist)"#);
    let result = exec(handler, &q).await;
    // Results come back as a set; order by distance is not guaranteed.
    sorted(int_column(&result, "Id"))
}

#[tokio::test]
async fn test_create_builds_index_and_query_works() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    setup(&handler).await;
    assert_eq!(nearest(&handler, "[1.0, 0.1, 0.0]", 2).await, vec![1, 4]);

    let stats = text(&exec(&handler, ".index stats doc_idx").await);
    assert!(
        stats.contains("vectors=4") && stats.contains("dimension=3"),
        "{stats}"
    );
}

#[tokio::test]
async fn test_query_results_join_with_base_relation() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    setup(&handler).await;
    let result = exec(
        &handler,
        r#"?hnsw_nearest("doc_idx", [0.0, 0.0, 1.0], 1, Id, Dist), docs(Id, Title, _)"#,
    )
    .await;
    assert_eq!(int_column(&result, "Id"), vec![3]);
    assert!(text(&result).contains('z'));
}

#[tokio::test]
async fn test_insert_makes_new_vector_findable() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    setup(&handler).await;
    exec(&handler, r#"+docs[(5, "neg", [-1.0, 0.0, 0.0])]"#).await;
    assert_eq!(nearest(&handler, "[-1.0, 0.05, 0.0]", 1).await, vec![5]);
    let stats = text(&exec(&handler, ".index stats doc_idx").await);
    assert!(stats.contains("vectors=5"), "{stats}");
}

#[tokio::test]
async fn test_delete_removes_vector_from_results() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    setup(&handler).await;
    exec(&handler, r#"-docs(1, "x", [1.0, 0.0, 0.0])"#).await;
    let got = nearest(&handler, "[1.0, 0.0, 0.0]", 2).await;
    assert!(!got.contains(&1), "{got:?}");
    assert_eq!(got.len(), 2);
    let stats = text(&exec(&handler, ".index stats doc_idx").await);
    assert!(
        stats.contains("vectors=3") && stats.contains("tombstones=1"),
        "{stats}"
    );
}

#[tokio::test]
async fn test_rebuild_drops_tombstones() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    setup(&handler).await;
    exec(&handler, r#"-docs(2, "y", [0.0, 1.0, 0.0])"#).await;
    let rebuilt = text(&exec(&handler, ".index rebuild doc_idx").await);
    assert!(
        rebuilt.contains("Index 'doc_idx' rebuilt (3 vectors)"),
        "{rebuilt}"
    );
    let stats = text(&exec(&handler, ".index stats doc_idx").await);
    assert!(stats.contains("tombstones=0"), "{stats}");
    assert_eq!(nearest(&handler, "[0.0, 1.0, 0.0]", 1).await, vec![4]);
}

#[tokio::test]
async fn test_index_survives_restart_and_matches_current_data() {
    let temp = TempDir::new().unwrap();
    {
        let handler = handler_at(temp.path());
        setup(&handler).await;
        exec(&handler, r#"+docs[(5, "neg", [-1.0, 0.0, 0.0])]"#).await;
        exec(&handler, r#"-docs(1, "x", [1.0, 0.0, 0.0])"#).await;
    }
    let handler = handler_at(temp.path());
    let list = text(&exec(&handler, ".index list").await);
    assert!(
        list.contains("Index 'doc_idx' on docs.emb") && list.contains("vectors: 4"),
        "{list}"
    );
    assert_eq!(nearest(&handler, "[-1.0, 0.0, 0.0]", 1).await, vec![5]);
    assert!(!nearest(&handler, "[1.0, 0.0, 0.0]", 4).await.contains(&1));

    // Dropped indexes stay dropped after restart.
    exec(&handler, ".index drop doc_idx").await;
    drop(handler);
    let handler = handler_at(temp.path());
    assert!(text(&exec(&handler, ".index list").await).contains("No indexes."));
}

#[tokio::test]
async fn test_bound_variable_query_vector() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    setup(&handler).await;
    exec(&handler, "+query_vec[([0.0, 0.1, 1.0])]").await;
    let result = exec(
        &handler,
        r#"?query_vec(QV), hnsw_nearest("doc_idx", QV, 1, Id, Dist)"#,
    )
    .await;
    assert_eq!(int_column(&result, "Id"), vec![3]);

    // A vector from another row of the indexed relation works as the query.
    let result = exec(
        &handler,
        r#"?docs(4, _, QV), hnsw_nearest("doc_idx", QV, 2, Id, Dist)"#,
    )
    .await;
    let got = sorted(int_column(&result, "Id"));
    assert!(got.contains(&4) && got.len() == 2, "{got:?}");
}

#[tokio::test]
async fn test_error_messages() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    setup(&handler).await;

    let err = failure(
        &handler,
        r#"?hnsw_nearest("nope", [1.0, 0.0, 0.0], 1, Id, D)"#,
    )
    .await;
    assert!(
        err.contains("index 'nope' does not exist") && err.contains("doc_idx"),
        "{err}"
    );

    let err = failure(
        &handler,
        r#"?hnsw_nearest("doc_idx", [1.0, 0.0], 1, Id, D)"#,
    )
    .await;
    assert!(
        err.contains("dimension 2") && err.contains("dimension 3"),
        "{err}"
    );

    let err = failure(&handler, r#"?hnsw_nearest("doc_idx", QV, 1, Id, D)"#).await;
    assert!(
        err.contains("QV") && err.contains("positive body atoms"),
        "{err}"
    );

    let msg = text(&exec(&handler, ".index create doc_idx on docs(emb)").await);
    assert!(msg.contains("already exists"), "{msg}");
    let msg = text(&exec(&handler, ".index create t_idx on docs(title)").await);
    assert!(msg.contains("needs a vector column"), "{msg}");
    let msg = text(&exec(&handler, ".index create n_idx on nothing(emb)").await);
    assert!(
        msg.contains("No schema found for relation 'nothing'"),
        "{msg}"
    );
    let msg = text(&exec(&handler, ".index rebuild ghost").await);
    assert!(
        msg.contains("Index 'ghost' not found") && msg.contains("doc_idx"),
        "{msg}"
    );

    // Rows the index cannot hold are rejected before they are stored.
    let err = failure(&handler, r#"+docs[(9, "bad", [1.0, 0.0])]"#).await;
    assert!(
        err.contains("dimension 2") && err.contains("row id=9"),
        "{err}"
    );
    let all = exec(&handler, "?docs(Id, T, V)").await;
    assert!(!int_column(&all, "id").contains(&9));
}

#[tokio::test]
async fn test_no_index_query_error() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    let err = failure(&handler, r#"?hnsw_nearest("doc_idx", [1.0], 1, Id, D)"#).await;
    assert!(
        err.contains("no vector index") && err.contains(".index create"),
        "{err}"
    );
}

/// Exact top-k agreement with brute-force cosine on a small dataset.
#[tokio::test]
async fn test_results_match_brute_force_cosine() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    exec(&handler, "+vecs(id: int, emb: vector)").await;
    let mut rng = StdRng::seed_from_u64(17);
    let rows: Vec<(i64, Vec<f32>)> = (0..150)
        .map(|i| (i, (0..8).map(|_| rng.gen_range(-1.0f32..1.0)).collect()))
        .collect();
    let facts: Vec<String> = rows
        .iter()
        .map(|(id, v)| format!("({id}, {})", vec_literal(v)))
        .collect();
    exec(&handler, &format!("+vecs[{}]", facts.join(", "))).await;
    exec(&handler, ".index create v_idx on vecs(emb) ef_search 200").await;

    let cosine = |a: &[f32], b: &[f32]| {
        let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
        let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        1.0 - dot / (na * nb)
    };
    for _ in 0..10 {
        let q: Vec<f32> = (0..8).map(|_| rng.gen_range(-1.0f32..1.0)).collect();
        let mut truth: Vec<(i64, f32)> = rows.iter().map(|(id, v)| (*id, cosine(&q, v))).collect();
        truth.sort_by(|a, b| a.1.total_cmp(&b.1));
        let expected = sorted(truth.iter().take(5).map(|(id, _)| *id).collect());
        let result = exec(
            &handler,
            &format!(r#"?hnsw_nearest("v_idx", {}, 5, Id, D)"#, vec_literal(&q)),
        )
        .await;
        assert_eq!(sorted(int_column(&result, "Id")), expected);
    }
}

/// A snapshot taken before a write keeps answering from the old index state.
#[test]
fn test_snapshot_sees_consistent_index() {
    let temp = TempDir::new().unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let handler = handler_at(temp.path());
    rt.block_on(setup(&handler));

    let query = r#"result(Id, D) <- hnsw_nearest("doc_idx", [-1.0, 0.0, 0.0], 1, Id, D)"#;
    let before = handler.get_storage().get_snapshot_for("default").unwrap();
    rt.block_on(exec(&handler, r#"+docs[(5, "neg", [-1.0, 0.0, 0.0])]"#));
    let after = handler.get_storage().get_snapshot_for("default").unwrap();

    let id_of = |snap: &inputlayer::storage_engine::KnowledgeGraphSnapshot| {
        let rows = snap.execute_tuples(query).unwrap();
        rows[0].get(0).cloned()
    };
    assert_ne!(id_of(&before), Some(inputlayer::value::Value::Int64(5)));
    assert_eq!(id_of(&after), Some(inputlayer::value::Value::Int64(5)));
}

/// Deleting past the tombstone threshold rebuilds the index automatically.
#[tokio::test]
async fn test_delete_past_threshold_compacts() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    exec(&handler, "+pts(id: int, emb: vector)").await;
    let rows: Vec<String> = (0..200)
        .map(|i| format!("({i}, [{}.0, 1.0])", i + 1))
        .collect();
    exec(&handler, &format!("+pts[{}]", rows.join(", "))).await;
    exec(&handler, ".index create p_idx on pts(emb) metric l2").await;
    let deletes: Vec<String> = (0..100)
        .map(|i| format!("({i}, [{}.0, 1.0])", i + 1))
        .collect();
    exec(&handler, &format!("-pts[{}]", deletes.join(", "))).await;
    let stats = text(&exec(&handler, ".index stats p_idx").await);
    let tombstones: usize = stats
        .split("tombstones=")
        .nth(1)
        .and_then(|rest| rest.split(',').next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("no tombstones in {stats}"));
    // Without compaction all 100 deletes would remain as tombstones.
    assert!(stats.contains("vectors=100") && tombstones < 64, "{stats}");
    let result = exec(&handler, r#"?hnsw_nearest("p_idx", [0.0, 1.0], 1, Id, D)"#).await;
    assert_eq!(int_column(&result, "Id"), vec![100]);
}

/// Dropping the relation drops its indexes.
#[tokio::test]
async fn test_drop_relation_drops_index() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    setup(&handler).await;
    exec(&handler, ".rel drop docs").await;
    assert!(text(&exec(&handler, ".index list").await).contains("No indexes."));
}
