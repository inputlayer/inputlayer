#![allow(clippy::unwrap_used)]

use super::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;
use std::sync::Arc;

fn config(metric: DistanceMetric) -> HnswConfig {
    HnswConfig {
        m: 16,
        ef_construction: 200,
        ef_search: 64,
        metric,
    }
}

fn random_rows(n: usize, dim: usize, seed: u64) -> Vec<(TupleId, Vec<f32>)> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|i| {
            let v = (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect();
            (i as TupleId, v)
        })
        .collect()
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f64 {
    let dot: f64 = a
        .iter()
        .zip(b)
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum();
    let na: f64 = a.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
    let nb: f64 = b.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
    1.0 - dot / (na * nb)
}

fn brute_force_top_k(rows: &[(TupleId, Vec<f32>)], query: &[f32], k: usize) -> Vec<TupleId> {
    let mut scored: Vec<(TupleId, f64)> = rows
        .iter()
        .map(|(id, v)| (*id, cosine_distance(query, v)))
        .collect();
    scored.sort_by(|a, b| a.1.total_cmp(&b.1));
    scored.into_iter().take(k).map(|(id, _)| id).collect()
}

fn ids(results: &[(TupleId, f64)]) -> Vec<TupleId> {
    results.iter().map(|(id, _)| *id).collect()
}

#[test]
fn test_hnsw_small_cosine_matches_brute_force_exactly() {
    let rows = random_rows(200, 8, 7);
    let index = HnswIndex::build(config(DistanceMetric::Cosine), rows.clone()).unwrap();
    let queries = random_rows(20, 8, 99);
    for (_, q) in &queries {
        let got = index.search(q, 5, Some(200), index.epoch()).unwrap();
        assert_eq!(ids(&got), brute_force_top_k(&rows, q, 5));
        // Distances are true cosine distances.
        for (id, dist) in &got {
            let expected = cosine_distance(q, &rows[*id as usize].1);
            assert!((dist - expected).abs() < 1e-4, "{dist} vs {expected}");
        }
    }
}

#[test]
fn test_hnsw_recall_at_10_on_3000_random_vectors() {
    let rows = random_rows(3000, 32, 42);
    let index = HnswIndex::build(config(DistanceMetric::Cosine), rows.clone()).unwrap();
    let queries = random_rows(100, 32, 4242);
    let k = 10;
    let mut found = 0;
    for (_, q) in &queries {
        let truth: HashSet<TupleId> = brute_force_top_k(&rows, q, k).into_iter().collect();
        let got = index.search(q, k, None, index.epoch()).unwrap();
        found += got.iter().filter(|(id, _)| truth.contains(id)).count();
    }
    let recall = found as f64 / (queries.len() * k) as f64;
    println!("hnsw recall@{k} (n=3000, dim=32, ef_search=64): {recall:.4}");
    assert!(recall >= 0.95, "recall {recall} < 0.95");
}

#[test]
fn test_hnsw_recall_holds_after_incremental_inserts() {
    let rows = random_rows(3000, 32, 5);
    let (first, rest) = rows.split_at(1500);
    let index = HnswIndex::build(config(DistanceMetric::Cosine), first.to_vec()).unwrap();
    for chunk in rest.chunks(100) {
        index.apply(chunk, &[]).unwrap();
    }
    assert_eq!(index.len(), 3000);
    let queries = random_rows(50, 32, 55);
    let mut found = 0;
    for (_, q) in &queries {
        let truth: HashSet<TupleId> = brute_force_top_k(&rows, q, 10).into_iter().collect();
        let got = index.search(q, 10, None, index.epoch()).unwrap();
        found += got.iter().filter(|(id, _)| truth.contains(id)).count();
    }
    let recall = found as f64 / 500.0;
    assert!(recall >= 0.95, "recall {recall} < 0.95");
}

#[test]
fn test_hnsw_apply_insert_makes_vector_findable() {
    let index = HnswIndex::build(
        config(DistanceMetric::Cosine),
        vec![(1, vec![1.0, 0.0, 0.0]), (2, vec![0.0, 1.0, 0.0])],
    )
    .unwrap();
    index.apply(&[(3, vec![0.0, 0.0, 1.0])], &[]).unwrap();
    let got = index
        .search(&[0.0, 0.1, 1.0], 1, None, index.epoch())
        .unwrap();
    assert_eq!(ids(&got), vec![3]);
    assert_eq!(index.len(), 3);
}

#[test]
fn test_hnsw_apply_delete_hides_vector_and_counts_tombstone() {
    let index = HnswIndex::build(
        config(DistanceMetric::Euclidean),
        vec![
            (1, vec![0.0, 0.0]),
            (2, vec![1.0, 0.0]),
            (3, vec![5.0, 5.0]),
        ],
    )
    .unwrap();
    index.apply(&[], &[1]).unwrap();
    let got = index.search(&[0.0, 0.0], 3, None, index.epoch()).unwrap();
    assert_eq!(ids(&got), vec![2, 3]);
    assert_eq!(index.len(), 2);
    assert_eq!(index.dead_count(), 1);
    assert!((index.dead_ratio() - 1.0 / 3.0).abs() < 1e-9);
}

#[test]
fn test_hnsw_upsert_replaces_vector() {
    let index =
        HnswIndex::build(config(DistanceMetric::Euclidean), vec![(1, vec![0.0, 0.0])]).unwrap();
    index.apply(&[(1, vec![10.0, 10.0])], &[]).unwrap();
    let got = index.search(&[10.0, 10.0], 5, None, index.epoch()).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0, 1);
    assert!(got[0].1 < 1e-6);
    assert_eq!(index.len(), 1);
}

#[test]
fn test_hnsw_old_epoch_sees_old_state() {
    let index =
        HnswIndex::build(config(DistanceMetric::Euclidean), vec![(1, vec![0.0, 0.0])]).unwrap();
    let e0 = index.epoch();
    let e1 = index.apply(&[(2, vec![0.1, 0.0])], &[1]).unwrap();
    assert!(e1 > e0);
    assert_eq!(
        ids(&index.search(&[0.0, 0.0], 5, None, e0).unwrap()),
        vec![1]
    );
    assert_eq!(
        ids(&index.search(&[0.0, 0.0], 5, None, e1).unwrap()),
        vec![2]
    );
}

#[test]
fn test_hnsw_search_finds_k_despite_many_tombstones() {
    // Above EXACT_SEARCH_MAX so the graph path (with result widening) runs.
    let rows = random_rows(2000, 8, 11);
    let index = HnswIndex::build(config(DistanceMetric::Cosine), rows.clone()).unwrap();
    let deletes: Vec<TupleId> = (0..1000).collect();
    index.apply(&[], &deletes).unwrap();
    let q = &rows[0].1;
    let got = index.search(q, 10, None, index.epoch()).unwrap();
    assert_eq!(got.len(), 10);
    assert!(got.iter().all(|(id, _)| *id >= 1000));
}

#[test]
fn test_hnsw_build_duplicate_ids_last_wins() {
    let index = HnswIndex::build(
        config(DistanceMetric::Euclidean),
        vec![(1, vec![0.0, 0.0]), (1, vec![3.0, 3.0])],
    )
    .unwrap();
    assert_eq!(index.len(), 1);
    let got = index.search(&[3.0, 3.0], 1, None, index.epoch()).unwrap();
    assert!(got[0].1 < 1e-6);
}

#[test]
fn test_hnsw_validation_errors() {
    let cos = HnswIndex::build(config(DistanceMetric::Cosine), vec![(1, vec![1.0, 0.0])]).unwrap();
    assert!(cos
        .validate(&[1.0, 0.0, 0.0])
        .unwrap_err()
        .contains("dimension 3"));
    assert!(cos.validate(&[0.0, 0.0]).unwrap_err().contains("zero-norm"));
    assert!(cos.validate(&[f32::NAN, 1.0]).unwrap_err().contains("NaN"));
    assert!(cos.validate(&[]).unwrap_err().contains("empty"));
    let err = cos.apply(&[(2, vec![1.0])], &[]).unwrap_err();
    assert!(err.contains("row id=2"), "{err}");
    // A failed batch changes nothing.
    assert_eq!(cos.len(), 1);

    let err = HnswIndex::build(config(DistanceMetric::Cosine), vec![(9, vec![0.0, 0.0])])
        .err()
        .unwrap();
    assert!(
        err.contains("row id=9") && err.contains("zero-norm"),
        "{err}"
    );
}

#[test]
fn test_hnsw_search_dimension_mismatch_errors() {
    let index =
        HnswIndex::build(config(DistanceMetric::Cosine), vec![(1, vec![1.0, 0.0])]).unwrap();
    let err = index
        .search(&[1.0, 0.0, 0.0], 1, None, index.epoch())
        .unwrap_err();
    assert!(
        err.contains("dimension 3") && err.contains("dimension 2"),
        "{err}"
    );
}

#[test]
fn test_hnsw_empty_index_returns_nothing() {
    let index = HnswIndex::new(config(DistanceMetric::Cosine));
    assert!(index.is_empty());
    assert_eq!(index.dimension(), 0);
    assert_eq!(index.search(&[1.0, 2.0], 3, None, 0).unwrap(), vec![]);
    // First insert fixes the dimension.
    index.apply(&[(1, vec![1.0, 2.0])], &[]).unwrap();
    assert_eq!(index.dimension(), 2);
}

#[test]
fn test_hnsw_all_metrics_rank_nearest_first() {
    let rows = vec![
        (1, vec![1.0, 0.0]),
        (2, vec![0.0, 1.0]),
        (3, vec![-1.0, 0.0]),
        (4, vec![0.7, 0.7]),
    ];
    for metric in [
        DistanceMetric::Cosine,
        DistanceMetric::Euclidean,
        DistanceMetric::DotProduct,
        DistanceMetric::Manhattan,
    ] {
        let index = HnswIndex::build(config(metric), rows.clone()).unwrap();
        let got = index.search(&[1.0, 0.05], 4, None, index.epoch()).unwrap();
        assert_eq!(got[0].0, 1, "metric {metric}");
        assert!(got.windows(2).all(|w| w[0].1 <= w[1].1), "metric {metric}");
    }
}

#[test]
fn test_hnsw_manhattan_reports_l1_distance() {
    let index = HnswIndex::build(
        config(DistanceMetric::Manhattan),
        vec![(1, vec![1.0, 2.0]), (2, vec![4.0, 6.0])],
    )
    .unwrap();
    let got = index.search(&[0.0, 0.0], 2, None, index.epoch()).unwrap();
    assert_eq!(got, vec![(1, 3.0), (2, 10.0)]);
}

#[test]
fn test_hnsw_concurrent_search_during_inserts() {
    let rows = random_rows(3000, 16, 3);
    let index =
        Arc::new(HnswIndex::build(config(DistanceMetric::Cosine), rows[..1200].to_vec()).unwrap());
    let frozen = index.epoch();
    let reader = {
        let index = Arc::clone(&index);
        let q = rows[0].1.clone();
        std::thread::spawn(move || {
            for _ in 0..200 {
                let got = index.search(&q, 5, None, frozen).unwrap();
                assert_eq!(got.len(), 5);
                assert!(got.iter().all(|(id, _)| *id < 1200), "saw a future entry");
            }
        })
    };
    for chunk in rows[1200..].chunks(100) {
        index.apply(chunk, &[]).unwrap();
    }
    reader.join().unwrap();
    assert_eq!(index.len(), 3000);
}

/// Regression: an unchecked `ef` sized `hnsw_rs`'s candidate heaps and
/// aborted the process; `k * 4` overflowed for Manhattan.
#[test]
fn test_hnsw_search_bounds_huge_k_and_ef() {
    let rows = random_rows(1500, 8, 7);
    for metric in [DistanceMetric::Euclidean, DistanceMetric::Manhattan] {
        let index = HnswIndex::build(config(metric), rows.clone()).unwrap();
        let (_, q) = &rows[0];
        let top = index.search(q, 5, Some(usize::MAX), index.epoch()).unwrap();
        assert_eq!(top.len(), 5);
        let all = index.search(q, usize::MAX, None, index.epoch()).unwrap();
        assert!(all.len() > 1400 && all.len() <= 1500, "{}", all.len());
    }
}

/// A definition saved before the parameters were bounded still builds:
/// `hnsw_rs` would exit the process on m > 256.
#[test]
fn test_hnsw_build_clamps_unbounded_saved_parameters() {
    let saved = HnswConfig {
        m: 100_000,
        ef_construction: usize::MAX,
        ef_search: usize::MAX,
        metric: DistanceMetric::Euclidean,
    };
    let rows = random_rows(1100, 4, 9);
    let index = HnswIndex::build(saved, rows.clone()).unwrap();
    let (_, q) = &rows[3];
    assert_eq!(index.search(q, 3, None, index.epoch()).unwrap().len(), 3);
}
