use serde_json::json;
use tempfile::TempDir;

use super::*;
use crate::protocol::subscription::Row;
use crate::Config;

const KG: &str = "groups";

fn handler(configure: impl FnOnce(&mut Config)) -> (Arc<Handler>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    configure(&mut config);
    let handler = Arc::new(Handler::from_config(config).unwrap());
    handler.get_storage().create_knowledge_graph(KG).unwrap();
    (handler, tmp)
}

async fn write(handler: &Handler, program: &str) {
    handler
        .execute_program(None, Some(KG.to_string()), program.to_string(), None)
        .await
        .unwrap();
}

fn values(rows: &[Row]) -> Vec<i64> {
    rows.iter().map(|row| row[0].as_i64().unwrap()).collect()
}

fn group(handler: &Arc<Handler>, queries: &[&str]) -> GroupQuery {
    GroupQuery::new(Arc::clone(handler), KG, queries).unwrap()
}

/// Refresh `query`, counting the queries the engine ran for it.
async fn refresh(handler: &Handler, query: &mut GroupQuery) -> (Refresh, u64) {
    let before = handler.total_queries();
    let refresh = query.refresh().await.unwrap();
    (refresh, handler.total_queries() - before)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_group_refresh_reports_every_query_at_one_revision() {
    let (handler, _tmp) = handler(|_| {});
    write(&handler, "+a[(1,), (2,)]\n+b[(7,)]").await;
    let mut query = group(&handler, &["?a(X)", "?b(Y)"]);
    let (first, ran) = refresh(&handler, &mut query).await;
    assert_eq!(ran, 2);
    let revision = handler.get_storage().get_snapshot_for(KG).unwrap().revision;
    assert_eq!(first.revision, revision);
    assert_eq!(values(&first.queries[0].inserted), [1, 2]);
    assert_eq!(first.queries[0].columns, ["X"]);
    assert_eq!(values(&first.queries[1].inserted), [7]);
    assert_eq!(first.queries[1].columns, ["Y"]);
    assert!(first.dependencies.contains("a") && first.dependencies.contains("b"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_query_whose_inputs_did_not_change_is_not_run_again() {
    let (handler, _tmp) = handler(|_| {});
    write(&handler, "+a(1)\n+b(1)").await;
    let mut query = group(&handler, &["?a(X)", "?b(X)"]);
    let (first, _) = refresh(&handler, &mut query).await;

    write(&handler, "+a(2)").await;
    let (second, ran) = refresh(&handler, &mut query).await;
    assert_eq!(ran, 1, "only the query reading `a` runs");
    assert!(second.revision > first.revision);
    assert_eq!(values(&second.queries[0].inserted), [2]);
    assert!(second.queries[1].is_unchanged());
    assert!(
        Arc::ptr_eq(&second.queries[1].result, &first.queries[1].result),
        "an unchanged result keeps its set"
    );

    let (third, ran) = refresh(&handler, &mut query).await;
    assert_eq!(ran, 0, "nothing changed: nothing runs");
    assert!(third.is_unchanged());
    assert!(third.revision >= second.revision);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_row_replaced_in_one_commit_is_seen_though_the_relation_keeps_its_size() {
    let (handler, _tmp) = handler(|_| {});
    write(&handler, "+a[(1,), (2,)]\n+b(1)").await;
    let mut query = group(&handler, &["?a(X)", "?b(X)"]);
    refresh(&handler, &mut query).await;
    write(&handler, "-a(1)\n+a(3)").await;
    let (replaced, ran) = refresh(&handler, &mut query).await;
    assert_eq!(ran, 1);
    assert_eq!(values(&replaced.queries[0].inserted), [3]);
    assert_eq!(values(&replaced.queries[0].retracted), [1]);
    assert!(replaced.queries[1].is_unchanged());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rerun_that_finds_the_same_rows_keeps_the_shared_result() {
    let (handler, _tmp) = handler(|_| {});
    write(&handler, "+a(1)\n+c(5)").await;
    write(&handler, "+r(X) <- a(X), c(_)").await;
    let mut query = group(&handler, &["?r(X)"]);
    let (first, _) = refresh(&handler, &mut query).await;
    write(&handler, "+c(6)").await;
    let (second, ran) = refresh(&handler, &mut query).await;
    assert_eq!(ran, 1, "`c` changed, so `r` runs");
    assert!(second.is_unchanged());
    assert!(Arc::ptr_eq(
        &second.queries[0].result,
        &first.queries[0].result
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rule_change_reruns_every_query_and_reports_only_real_changes() {
    let (handler, _tmp) = handler(|_| {});
    write(&handler, "+a(1)\n+b(2)").await;
    write(&handler, "+r(X) <- a(X)").await;
    let mut query = group(&handler, &["?r(X)", "?b(X)"]);
    refresh(&handler, &mut query).await;

    // The change log tracks the rules as a whole: every query runs again.
    write(&handler, "+r(X) <- b(X)").await;
    let (refresh_1, ran) = refresh(&handler, &mut query).await;
    assert_eq!(ran, 2);
    assert_eq!(values(&refresh_1.queries[0].inserted), [2]);
    assert!(refresh_1.queries[1].is_unchanged());
    assert!(refresh_1.dependencies.contains("b"));

    let (refresh_2, ran) = refresh(&handler, &mut query).await;
    assert_eq!(ran, 0, "nothing changed since: nothing runs");
    assert!(refresh_2.is_unchanged());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_query_fails_the_refresh_and_keeps_every_result() {
    let (handler, _tmp) = handler(|config| config.storage.performance.max_result_rows = 3);
    write(&handler, "+a(1)\n+b(1)").await;
    let mut query = group(&handler, &["?a(X)", "?b(X)"]);
    refresh(&handler, &mut query).await;

    // `b` changes in the same commit that pushes `a` past the result cap.
    write(&handler, "+a[(2,), (3,), (4,)]\n+b(2)").await;
    let error = query.refresh().await.unwrap_err();
    assert!(error.starts_with("?a(X): "), "{error}");
    assert!(error.contains("max_result_rows"), "{error}");

    // Both changes are reported against the last complete results.
    write(&handler, "-a(4)").await;
    let (recovered, _) = refresh(&handler, &mut query).await;
    assert_eq!(values(&recovered.queries[0].inserted), [2, 3]);
    assert_eq!(values(&recovered.queries[1].inserted), [2]);
    assert!(recovered.queries.iter().all(|q| q.retracted.is_empty()));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_plain_query_fails_without_naming_itself() {
    let (handler, _tmp) = handler(|config| config.storage.performance.max_result_rows = 1);
    write(&handler, "+a[(1,), (2,)]").await;
    let error = group(&handler, &["?a(X)"]).refresh().await.unwrap_err();
    assert!(error.starts_with("Subscription result exceeds"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delete_and_reinsert_in_one_commit_is_no_change() {
    let (handler, _tmp) = handler(|_| {});
    write(&handler, "+a(1)\n+b(1)").await;
    let mut query = group(&handler, &["?a(X)", "?b(X)"]);
    refresh(&handler, &mut query).await;
    write(&handler, "-a(1)\n+a(1)").await;
    let (again, ran) = refresh(&handler, &mut query).await;
    assert!(ran <= 1, "`b` did not change");
    assert!(again.is_unchanged(), "same rows: {:?}", again.queries[0]);
    assert_eq!(again.queries[0].result.sorted_rows(), [vec![json!(1)]]);
}

#[test]
fn an_empty_group_is_refused() {
    let tmp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    let handler = Arc::new(Handler::from_config(config).unwrap());
    assert!(GroupQuery::new(Arc::clone(&handler), KG, &[]).is_err());
    let error = GroupQuery::new(handler, KG, &["?a(X)", "a(X)"])
        .err()
        .unwrap();
    assert!(error.contains("must start with '?'"), "{error}");
}
