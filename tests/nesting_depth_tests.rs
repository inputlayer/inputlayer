//! Input size limits that keep recursion bounded: deep or oversized input is
//! a validation error, never a stack overflow that aborts the process.
//!
//! Requests run as the server runs them, on runtime threads with
//! `ENGINE_THREAD_STACK_BYTES` stacks. Input that got past a limit would
//! overflow and crash this test binary.

use inputlayer::parser::{
    max_nesting_depth, set_max_nesting_depth, DEFAULT_MAX_NESTING_DEPTH, MAX_NESTING_DEPTH_CEILING,
    MAX_RULE_BODY_SIZE,
};
use inputlayer::protocol::handler::VALIDATION_ERROR_PREFIX;
use inputlayer::protocol::{ErrorCode, Handler, ProgramError, QueryResult};
use inputlayer::{Config, StorageEngine, ENGINE_THREAD_STACK_BYTES};
use std::future::Future;
use std::path::Path;
use std::sync::Mutex;
use tempfile::TempDir;

/// The nesting limit is process-wide: tests that depend on it run one at a
/// time.
static LIMIT: Mutex<()> = Mutex::new(());

/// Run `test` on a runtime configured like the server's.
fn on_engine_threads<F>(test: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let _limit = LIMIT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _reset = ResetLimit;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(ENGINE_THREAD_STACK_BYTES)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(runtime.spawn(test)).expect("test task");
}

/// Restores the default nesting limit, even when a test fails.
struct ResetLimit;

impl Drop for ResetLimit {
    fn drop(&mut self) {
        set_max_nesting_depth(DEFAULT_MAX_NESTING_DEPTH);
    }
}

fn open(dir: &Path) -> Handler {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    Handler::new(StorageEngine::new(config).expect("create storage engine"))
}

fn handler() -> (Handler, TempDir) {
    let temp = TempDir::new().expect("create temp dir");
    (open(temp.path()), temp)
}

async fn run(handler: &Handler, program: &str) -> Result<QueryResult, ProgramError> {
    handler
        .execute_program_status(
            None,
            Some("default".to_string()),
            program.to_string(),
            None,
            &handler.request_control(None),
        )
        .await
}

/// Run `program` and require every statement to succeed.
async fn ok(handler: &Handler, program: &str) -> QueryResult {
    let shown = &program[..program.len().min(60)];
    let result = run(handler, program)
        .await
        .unwrap_or_else(|e| panic!("{shown}...: {e:?}"));
    assert!(result.errors.is_empty(), "{shown}...: {:?}", result.errors);
    result
}

/// The single value `?{relation}(X)` returns.
async fn value(handler: &Handler, relation: &str) -> String {
    let result = ok(handler, &format!("?{relation}(X)")).await;
    assert_eq!(result.rows.len(), 1, "{relation}: {result:?}");
    format!("{:?}", result.rows[0].values[0])
}

fn assert_refused(result: Result<QueryResult, ProgramError>, program: &str, reason: &str) {
    let shown = &program[..program.len().min(60)];
    let err = result.expect_err(&format!("{shown}...: must be refused"));
    // A statement that fails to parse fails the program before anything
    // runs, reported as structured validation errors.
    assert!(
        err.code == Some(ErrorCode::Validation) || err.message.starts_with(VALIDATION_ERROR_PREFIX),
        "{shown}...: {err:?}"
    );
    assert!(err.message.contains(reason), "{shown}...: {err:?}");
}

fn assert_too_deep(result: Result<QueryResult, ProgramError>, program: &str) {
    assert_refused(result, program, "nesting exceeds the limit");
}

/// `abs(abs(...abs(inner)...))`, `levels` calls deep.
fn calls(levels: usize, inner: &str) -> String {
    format!("{}{inner}{}", "abs(".repeat(levels), ")".repeat(levels))
}

/// `((...(inner)...))`, `levels` groups deep.
fn parens(levels: usize, inner: &str) -> String {
    format!("{}{inner}{}", "(".repeat(levels), ")".repeat(levels))
}

/// `inner+1+1...`, `ops` additions long (left-deep, one level per operator).
fn sum_chain(ops: usize, inner: &str) -> String {
    format!("{inner}{}", "+1".repeat(ops))
}

/// The audit's repro (4,000 nested calls killed the server) and worse, on
/// every statement form that parses terms.
#[test]
fn deep_terms_are_refused_on_every_statement_form() {
    on_engine_threads(async {
        let (handler, _tmp) = handler();
        for term in [
            calls(4_000, "1"),
            calls(100_000, "1"),
            parens(100_000, "Y+1"),
            sum_chain(100_000, "Y"),
            format!("Y*{}", parens(50_000, "Y*2")),
        ] {
            for program in [
                format!("+r({term})"),
                format!("-r({term})"),
                format!("?r(Y), X = {term}"),
                format!("+p(X) <- r(Y), X = {term}"),
                format!("p(X) <- r(Y), X = {term}"),
                format!("+p(X) <- r(Y), {term} > X"),
            ] {
                assert_too_deep(run(&handler, &program).await, &program);
            }
        }
        // The engine is still up and serving.
        ok(&handler, "+r(5)").await;
        assert_eq!(value(&handler, "r").await, "Int64(5)");
    });
}

/// Terms exactly at the limit parse and evaluate end to end; one level more
/// is refused.
#[test]
fn nesting_limit_is_inclusive() {
    on_engine_threads(async {
        assert_eq!(max_nesting_depth(), DEFAULT_MAX_NESTING_DEPTH);
        let (handler, _tmp) = handler();
        ok(&handler, "+r(-3)").await;
        let depth = DEFAULT_MAX_NESTING_DEPTH;
        // Each case: (term at the limit, one level deeper, its value).
        let cases = [
            (calls(depth, "Y"), calls(depth + 1, "Y"), "Int64(3)"),
            // The `+` inside the groups is a level of its own.
            (parens(depth - 1, "Y+1"), parens(depth, "Y+1"), "Int64(-2)"),
            (
                sum_chain(depth, "Y"),
                sum_chain(depth + 1, "Y"),
                "Int64(125)",
            ),
        ];
        for (i, (at_limit, past_limit, want)) in cases.iter().enumerate() {
            ok(&handler, &format!("+p{i}(X) <- r(Y), X = {at_limit}")).await;
            assert_eq!(value(&handler, &format!("p{i}")).await, *want);
            let rule = format!("+q{i}(X) <- r(Y), X = {past_limit}");
            assert_too_deep(run(&handler, &rule).await, &rule);
        }
    });
}

/// The highest configurable limit still fits every pass over the term, and
/// the configured limit is the one enforced.
#[test]
fn configured_nesting_limit_is_enforced_up_to_the_ceiling() {
    on_engine_threads(async {
        let (handler, _tmp) = handler();
        ok(&handler, "+r(-3)").await;

        set_max_nesting_depth(usize::MAX);
        assert_eq!(max_nesting_depth(), MAX_NESTING_DEPTH_CEILING);
        let depth = MAX_NESTING_DEPTH_CEILING;
        let sum = format!("Int64({})", depth - 3);
        let cases = [
            (calls(depth, "Y"), "Int64(3)"),
            (parens(depth - 1, "Y+1"), "Int64(-2)"),
            (sum_chain(depth, "Y"), sum.as_str()),
        ];
        for (i, (term, want)) in cases.iter().enumerate() {
            ok(&handler, &format!("+c{i}(X) <- r(Y), X = {term}")).await;
            assert_eq!(value(&handler, &format!("c{i}")).await, *want);
        }
        let program = format!("+r({})", calls(depth + 1, "1"));
        assert_too_deep(run(&handler, &program).await, &program);

        set_max_nesting_depth(4);
        let program = format!("+r({})", calls(5, "1"));
        assert_too_deep(run(&handler, &program).await, &program);
        let err = run(&handler, &format!("+r({})", calls(4, "1")))
            .await
            .expect_err("a call is still not a constant");
        assert!(!err.message.contains("nesting"), "{err:?}");

        set_max_nesting_depth(0);
        assert_eq!(max_nesting_depth(), 1);
    });
}

/// Rules at the nesting limit survive a restart. Their stored form nests
/// past `serde_json`'s default limit of 128, which once left the write-ahead
/// log unreadable and the engine unable to start.
#[test]
fn rules_at_the_limits_survive_a_restart() {
    on_engine_threads(async {
        let temp = TempDir::new().expect("create temp dir");
        let depth = DEFAULT_MAX_NESTING_DEPTH;
        let wide = format!("r(Y), {}", vec!["Y > -9"; 510].join(", "));
        {
            let handler = open(temp.path());
            ok(&handler, "+r(-3)").await;
            ok(
                &handler,
                &format!("+a(X) <- r(Y), X = {}", calls(depth, "Y")),
            )
            .await;
            ok(
                &handler,
                &format!("+b(X) <- r(Y), X = {}", sum_chain(depth, "Y")),
            )
            .await;
            ok(&handler, &format!("+w(Y) <- {wide}")).await;
        }
        let handler = open(temp.path());
        assert_eq!(value(&handler, "a").await, "Int64(3)");
        assert_eq!(value(&handler, "b").await, "Int64(125)");
        assert_eq!(value(&handler, "w").await, "Int64(-3)");
    });
}

/// Bodies at the size limit plan and evaluate end to end; one element more
/// is refused, in every shape that deepens the plan.
#[test]
fn rule_body_size_limit_is_inclusive() {
    on_engine_threads(async {
        let (handler, _tmp) = handler();
        ok(&handler, "+r(-3)").await;
        ok(&handler, "+q(7)").await;
        ok(&handler, &format!("+s({}-3)", "1, ".repeat(510))).await;
        let n = MAX_RULE_BODY_SIZE;
        let joins = |atoms: usize| vec!["r(Y)"; atoms].join(", ");
        let comparisons = |count: usize| format!("r(Y), {}", vec!["Y < 9"; count].join(", "));
        let negations = |count: usize| format!("r(Y), {}", vec!["!q(Y)"; count].join(", "));
        let constants = |count: usize| format!("s({}Y)", "1, ".repeat(count));
        // Joins and wide atoms at the limit are too slow to evaluate here.
        let at_limit = [comparisons(n - 2), negations(n / 2 - 1), constants(510)];
        for (i, body) in at_limit.iter().enumerate() {
            ok(&handler, &format!("+p{i}(Y) <- {body}")).await;
            assert_eq!(value(&handler, &format!("p{i}")).await, "Int64(-3)");
        }
        let past_limit = [
            comparisons(n - 1),
            format!("{}, Y < 9", negations(n / 2 - 1)),
            format!("{}, Y < 9", joins(n / 2)),
            constants(n - 1),
        ];
        for (i, body) in past_limit.iter().enumerate() {
            for program in [format!("+x{i}(Y) <- {body}"), format!("?{body}")] {
                assert_refused(run(&handler, &program).await, &program, "body is too large");
            }
        }
        let program = format!("?{}", joins(100_000));
        assert_refused(run(&handler, &program).await, &program, "body is too large");
    });
}

/// Type expressions share the nesting limit.
#[test]
fn deep_type_expressions_are_refused() {
    on_engine_threads(async {
        let (handler, _tmp) = handler();
        let list = |depth: usize| format!("{}int{}", "list[".repeat(depth), "]".repeat(depth));
        let record = |depth: usize| format!("{}int{}", "{ a: ".repeat(depth), " }".repeat(depth));
        let depth = DEFAULT_MAX_NESTING_DEPTH;
        ok(&handler, &format!("type Deep: {}", list(depth))).await;
        ok(&handler, &format!("type Rec: {}", record(depth))).await;
        for program in [
            format!("type Deeper: {}", list(depth + 1)),
            format!("type Deeper: {}", list(100_000)),
            format!("type Deeper: {}", record(100_000)),
            format!("+t(a: {})", list(100_000)),
        ] {
            assert_too_deep(run(&handler, &program).await, &program);
        }
    });
}

#[test]
fn config_clamps_the_nesting_limit() {
    let mut config = Config::default();
    assert_eq!(
        config.storage.performance.max_nesting_depth,
        DEFAULT_MAX_NESTING_DEPTH
    );
    config.storage.performance.max_nesting_depth = 0;
    config.validate().expect("valid");
    assert_eq!(
        config.storage.performance.max_nesting_depth,
        DEFAULT_MAX_NESTING_DEPTH
    );
    config.storage.performance.max_nesting_depth = MAX_NESTING_DEPTH_CEILING + 1;
    config.validate().expect("valid");
    assert_eq!(
        config.storage.performance.max_nesting_depth,
        MAX_NESTING_DEPTH_CEILING
    );
}
