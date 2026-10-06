//! Every `_` in a rule body is a fresh anonymous variable, end to end
//! through `Handler`, for session rules, persistent rules and persistent
//! rules reloaded after a restart.
//!
//! Lowering used to name a `_` column after its relation and position, so two
//! atoms over one relation with `_` at the same position joined on it:
//! `w(A,B) <- user(A,_,_), user(B,_,_)` returned only the diagonal.

use crate::harness::handler_at;
use inputlayer::protocol::Handler;
use tempfile::TempDir;

async fn exec(handler: &Handler, program: &str) {
    handler
        .query_program(None, program.to_string())
        .await
        .unwrap_or_else(|e| panic!("Failed to execute '{program}': {e}"));
}

fn sorted(result: inputlayer::protocol::wire::QueryResult) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = result
        .rows
        .iter()
        .map(|row| row.values.iter().map(|v| format!("{v}")).collect())
        .collect();
    rows.sort();
    rows
}

async fn rows(handler: &Handler, query: &str) -> Vec<Vec<String>> {
    sorted(
        handler
            .query_program(None, query.to_string())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute '{query}': {e}")),
    )
}

fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = rows
        .iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect();
    out.sort();
    out
}

const USERS: &str = r#"+user[(1,"ann",true),(2,"bob",false),(3,"cy",true)]"#;

/// All nine (A, B) pairs: the `_` columns of the two atoms are independent.
fn all_user_pairs() -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for a in 1..=3 {
        for b in 1..=3 {
            out.push(vec![a.to_string(), b.to_string()]);
        }
    }
    out
}

#[tokio::test]
async fn persistent_rule_wildcards_do_not_join() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    exec(&handler, USERS).await;
    exec(&handler, "+w(A,B) <- user(A,_,_), user(B,_,_)").await;
    assert_eq!(rows(&handler, "?w(A,B)").await, all_user_pairs());
}

#[tokio::test]
async fn session_rule_wildcards_do_not_join() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    exec(&handler, USERS).await;
    assert_eq!(
        rows(&handler, "w(A,B) <- user(A,_,_), user(B,_,_)\n?w(A,B)").await,
        all_user_pairs()
    );
}

#[tokio::test]
async fn persistent_wildcard_rule_survives_restart() {
    let temp = TempDir::new().unwrap();
    {
        let handler = handler_at(temp.path());
        exec(&handler, USERS).await;
        exec(&handler, "+w(A,B) <- user(A,_,_), user(B,_,_)").await;
    }
    let handler = handler_at(temp.path());
    assert_eq!(rows(&handler, "?w(A,B)").await, all_user_pairs());
}

/// Two-hop paths through a labelled edge: the labels of the two hops differ.
#[tokio::test]
async fn wildcards_in_a_chained_join_do_not_join() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    exec(&handler, r#"+e[(1,2,"a"),(2,3,"b"),(3,4,"c")]"#).await;
    exec(&handler, "+two(X,Z) <- e(X,Y,_), e(Y,Z,_)").await;
    assert_eq!(
        rows(&handler, "?two(X,Z)").await,
        table(&[&["1", "3"], &["2", "4"]])
    );
}

/// A `_` in a negated atom is existential and never joins a positive `_`.
#[tokio::test]
async fn wildcards_in_negated_atoms() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    exec(&handler, "+follows[(1,10),(2,20),(3,10)]").await;
    exec(&handler, "+muted[(1,99)]").await;
    exec(
        &handler,
        "+pair(A,B) <- follows(A,_), follows(B,_), !muted(A,_)",
    )
    .await;
    assert_eq!(
        rows(&handler, "?pair(A,B)").await,
        table(&[
            &["2", "1"],
            &["2", "2"],
            &["2", "3"],
            &["3", "1"],
            &["3", "2"],
            &["3", "3"],
        ])
    );
}

#[tokio::test]
async fn wildcards_under_an_aggregate() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    exec(&handler, USERS).await;
    exec(&handler, "+peers(A, count<B>) <- user(A,_,_), user(B,_,_)").await;
    assert_eq!(
        rows(&handler, "?peers(A,N)").await,
        table(&[&["1", "3"], &["2", "3"], &["3", "3"]])
    );
}

/// The recursive step composes two hops whose labels differ.
#[tokio::test]
async fn wildcards_in_a_recursive_rule() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    exec(&handler, r#"+e[(1,2,"a"),(2,3,"b"),(3,4,"c")]"#).await;
    exec(&handler, "+r(X,Y,L) <- e(X,Y,L)").await;
    exec(&handler, r#"+r(X,Z,"via") <- r(X,Y,_), r(Y,Z,_)"#).await;
    exec(&handler, "+reach(X,Y) <- r(X,Y,_)").await;
    assert_eq!(
        rows(&handler, "?reach(X,Y)").await,
        table(&[
            &["1", "2"],
            &["1", "3"],
            &["1", "4"],
            &["2", "3"],
            &["2", "4"],
            &["3", "4"],
        ])
    );
}
