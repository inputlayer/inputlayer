//! Typed arithmetic in comparisons and assignments, and expression
//! arguments in body atoms.

use inputlayer::protocol::{Handler, WireValue};
use inputlayer::Config;
use tempfile::TempDir;

fn handler() -> (Handler, TempDir) {
    let temp = TempDir::new().expect("temp dir");
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    let storage = inputlayer::StorageEngine::new(config).expect("storage engine");
    (Handler::new(storage), temp)
}

async fn exec(program: &[&str], query: &str) -> Result<Vec<Vec<WireValue>>, String> {
    let (h, _t) = handler();
    for stmt in program {
        h.query_program(None, (*stmt).to_string())
            .await
            .unwrap_or_else(|e| panic!("'{stmt}': {e}"));
    }
    let result = h.query_program(None, query.to_string()).await?;
    let mut rows: Vec<Vec<WireValue>> = result.rows.into_iter().map(|r| r.values).collect();
    rows.sort_by_key(|r| format!("{r:?}"));
    Ok(rows)
}

async fn run(program: &[&str], query: &str) -> Vec<Vec<WireValue>> {
    exec(program, query)
        .await
        .unwrap_or_else(|e| panic!("'{query}': {e}"))
}

async fn first_col(program: &[&str], query: &str) -> Vec<WireValue> {
    run(program, query)
        .await
        .into_iter()
        .map(|r| r[0].clone())
        .collect()
}

fn int(v: i64) -> WireValue {
    WireValue::Int64(v)
}

#[tokio::test]
async fn float_columns_in_comparison_arithmetic() {
    let program = ["+item[(1, 3.0, 1.0), (2, 1.0, 1.0)]"];
    assert_eq!(
        first_col(&program, "?item(I, S, T), S > T * 2").await,
        vec![int(1)]
    );
    assert_eq!(
        first_col(&program, "?item(I, S, T), T * 2 < S").await,
        vec![int(1)]
    );
}

#[tokio::test]
async fn float_constants_not_truncated_when_folded() {
    let program = ["+v[(1,), (2,)]"];
    assert_eq!(
        first_col(&program, "?v(X), X > 0.5 * 3").await,
        vec![int(2)]
    );
    assert_eq!(first_col(&program, "?v(X), X < 3 / 2").await, vec![int(1)]);
}

#[tokio::test]
async fn comparison_overflow_does_not_wrap() {
    let program = ["+v[(9223372036854775807,)]"];
    assert!(run(&program, "?v(X), X + 1 < 0").await.is_empty());
    assert!(run(&program, "?v(X), X * 2 < 0").await.is_empty());
    assert!(run(&program, "?v(X), X + 1 > 0").await.is_empty());
}

#[tokio::test]
async fn min_int_divided_by_minus_one_does_not_panic() {
    let program = ["+v[(-9223372036854775807,)]", "+m(Y) <- v(X), Y = X - 1"];
    assert_eq!(
        first_col(&program, "?m(Y), Y / -1 > 0").await,
        vec![int(i64::MIN)]
    );
    assert!(run(&program, "?m(Y), Y % -1 = 0").await.is_empty());
    assert_eq!(
        first_col(&program, "?m(Y), Y < (0 - 9223372036854775807 - 1) / -1").await,
        vec![int(i64::MIN)]
    );
}

#[tokio::test]
async fn large_int_assignment_is_exact() {
    let x = (1i64 << 60) + 1;
    let program = [format!("+v[({x},)]")];
    let program: Vec<&str> = program.iter().map(String::as_str).collect();
    assert_eq!(
        run(&program, "?v(X), Y = X + 1").await,
        vec![vec![int(x), int(x + 1)]]
    );
}

#[tokio::test]
async fn non_numeric_operand_gives_null() {
    let program = ["+v[(\"abc\",)]"];
    assert_eq!(
        run(&program, "?v(X), Y = X + 1").await,
        vec![vec![WireValue::String("abc".into()), WireValue::Null]]
    );
    assert!(run(&program, "?v(X), X + 1 > 0").await.is_empty());
}

#[tokio::test]
async fn int_overflow_in_assignment_gives_null() {
    let program = ["+v[(9223372036854775807,)]"];
    assert_eq!(
        run(&program, "?v(X), Y = X + 1").await,
        vec![vec![int(i64::MAX), WireValue::Null]]
    );
}

#[tokio::test]
async fn expression_argument_in_body_atom_is_rejected() {
    let program = ["+e[(1, 2)]", "+v[(2,)]"];
    for query in ["?v(Y), e(X, Y + 1)", "?v(Y), e(1, X), !e(X, Y + 1)"] {
        let err = exec(&program, query).await.expect_err(query);
        assert!(err.contains("not supported"), "{query}: {err}");
    }
    exec(&program, "?v(Y), e(X, abs_int64(Y))")
        .await
        .expect_err("function call argument");
    let (h, _t) = handler();
    let err = h
        .query_program(None, "+r(X) <- v(Y), e(X, Y + 1)".to_string())
        .await
        .expect_err("persistent rule");
    assert!(err.contains("not supported"), "{err}");
}

#[tokio::test]
async fn timestamp_column_against_float_arithmetic() {
    let program = ["+raw[(1, 1000, 990, 30), (2, 2000, 900, 20)]"];
    let ts = "?raw(I, X, S, D), T = time_add(X, 0)";
    assert_eq!(
        first_col(&program, &format!("{ts}, T < S + D / 2")).await,
        vec![int(1)]
    );
    assert_eq!(
        first_col(&program, &format!("{ts}, T > 1000 * 1.5")).await,
        vec![int(2)]
    );
    assert_eq!(
        first_col(&program, &format!("{ts}, T > S + 0.5")).await,
        vec![int(1), int(2)]
    );
}

#[tokio::test]
async fn null_fails_ne_folded_or_not() {
    let program = ["+v[(1, 1), (2, \"abc\")]", "+m(I, N) <- v(I, X), N = X + 1"];
    assert_eq!(
        first_col(&program, "?m(I, N), N != 1 + 2").await,
        vec![int(1)]
    );
    assert_eq!(
        first_col(&program, "?m(I, N), N != I + 2").await,
        vec![int(1)]
    );
}
