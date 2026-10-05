//! `.iql.out` snapshot cases as oracle histories.
//!
//! A snapshot transcript echoes each statement on one `> ` line followed by
//! its output, so it yields both the history and the recorded results. The
//! recorded tables join the comparison as the `spec` adapter: the reference
//! checks the spec where it models the case, and the subscription path must
//! reproduce the spec at every query point.
//!
//! Cases that need what a history cannot express (several knowledge graphs,
//! session state, file loads, indexes) are listed as unsupported with the
//! reason, not silently dropped.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use inputlayer::statement::{parse_statement, MetaCommand, Statement};

use crate::adapter::Adapter;
use crate::model::{AdapterError, Cell, History, Observation, Outcome, Revision, Row, Step};
use crate::oracle::{self, Report};

/// Snapshot categories whose cases exercise derived results.
const CATEGORIES: &[&str] = &[
    "06_joins",
    "07_filters",
    "08_negation",
    "09_recursion",
    "10_edge_cases",
    "14_aggregations",
    "17_rule_commands",
    "18_advanced_patterns",
    "22_set_operations",
    "27_atomic_ops",
    "90_product_fixtures/42_consistency_pack",
];

/// Cases whose engine evaluation alone takes tens of seconds in a debug
/// build; the snapshot tests cover them. Reported as unsupported.
const SLOW_CASES: &[&str] = &["09_recursion/07_deep_recursion_500.iql.out"];

pub struct Case {
    pub history: History,
    expected: BTreeMap<(Revision, String), Observation>,
}

/// Recorded query results of one case.
pub struct SpecAdapter {
    expected: BTreeMap<(Revision, String), Observation>,
}

impl Adapter for SpecAdapter {
    fn name(&self) -> &'static str {
        "spec"
    }

    fn execute(&mut self, _statement: &str, _revision: Revision) -> Result<Outcome, AdapterError> {
        Ok(Outcome::Assumed)
    }

    fn restart(&mut self, _revision: Revision) -> Result<(), AdapterError> {
        Ok(())
    }

    fn observe(&mut self, query: &str, revision: Revision) -> Result<Observation, AdapterError> {
        self.expected
            .get(&(revision, query.to_string()))
            .cloned()
            .ok_or_else(|| AdapterError::Unsupported("not recorded at this revision".into()))
    }

    /// The spec records results; it derives no state that could go stale.
    fn desync(&mut self, _reason: String) {}
}

/// What a statement means for the history.
enum Role {
    Step,
    /// Read-only: does not change state.
    Skip,
    Unsupported(String),
}

fn role(statement: &str) -> Role {
    match parse_statement(statement) {
        Err(_) => Role::Step, // rejected by every engine adapter alike
        Ok(Statement::SessionRule(_) | Statement::Fact(_)) => {
            Role::Unsupported("session state".into())
        }
        Ok(Statement::Meta(meta)) => match meta {
            MetaCommand::RuleDrop(_)
            | MetaCommand::RuleDropPrefix(_)
            | MetaCommand::RuleClear(_)
            | MetaCommand::RuleRemove { .. }
            | MetaCommand::RuleEdit { .. }
            | MetaCommand::RelDrop(_)
            | MetaCommand::ClearPrefix(_)
            | MetaCommand::Compact => Role::Step,
            MetaCommand::KgShow
            | MetaCommand::KgList
            | MetaCommand::RelList
            | MetaCommand::RelDescribe(_)
            | MetaCommand::RuleList
            | MetaCommand::RuleQuery(_)
            | MetaCommand::RuleShowDef(_)
            | MetaCommand::Status
            | MetaCommand::Debug(_)
            | MetaCommand::Why(_)
            | MetaCommand::WhyFull(_)
            | MetaCommand::WhyNot(_)
            | MetaCommand::Help
            | MetaCommand::IndexList
            | MetaCommand::IndexStats(_) => Role::Skip,
            other => Role::Unsupported(format!("meta command {other:?}")),
        },
        Ok(_) => Role::Step,
    }
}

/// Parse one transcript into a case, or the reason it cannot be one.
pub fn load(transcript: &str) -> Result<Case, String> {
    let mut blocks: Vec<(&str, Vec<&str>)> = Vec::new();
    for line in transcript.lines() {
        if let Some(statement) = line.strip_prefix("> ") {
            blocks.push((statement.trim(), Vec::new()));
        } else if let Some((_, output)) = blocks.last_mut() {
            output.push(line);
        }
    }
    let mut steps = Vec::new();
    let mut expected = BTreeMap::new();
    let mut queries = BTreeSet::new();
    let mut failing = BTreeSet::new();
    let mut kg: Option<&str> = None;
    for (statement, output) in blocks {
        if let Some(command) = statement.strip_prefix(".kg ") {
            match (
                command.split_whitespace().collect::<Vec<_>>().as_slice(),
                kg,
            ) {
                (["create", name], None) => kg = Some(name),
                (["use", name], Some(current)) if *name == current => {}
                (["use", "default"], Some(_)) => break, // cleanup follows
                _ => return Err(format!("knowledge graph command `{statement}`")),
            }
            continue;
        }
        if kg.is_none() {
            return Err(format!("`{statement}` before the case's knowledge graph"));
        }
        if statement.starts_with('?') {
            steps.push(Step::Checkpoint(format!(
                "`{statement}` #{}",
                steps.len() + 1
            )));
            queries.insert(statement.to_string());
            match recorded(&output) {
                Recorded::Rows(rows) => {
                    expected.insert((Revision(steps.len()), statement.to_string()), rows);
                }
                Recorded::Unreadable => {}
                Recorded::Error => {
                    failing.insert(statement.to_string());
                }
            }
            continue;
        }
        match role(statement) {
            Role::Step => steps.push(Step::Execute(statement.to_string())),
            Role::Skip => {}
            Role::Unsupported(reason) => return Err(format!("{reason}: `{statement}`")),
        }
    }
    // A query the spec records as an error is not observed at all: the oracle
    // compares results, and error texts are covered by the snapshot tests.
    expected.retain(|(_, query), _| !failing.contains(query));
    Ok(Case {
        history: History {
            queries: queries.difference(&failing).cloned().collect(),
            steps,
        },
        expected,
    })
}

/// What a transcript recorded for one query.
enum Recorded {
    Rows(Observation),
    /// A table the oracle cannot read back exactly (display-truncated cells,
    /// a row count that does not match): observed, but not checked against.
    Unreadable,
    /// No result table: the query failed.
    Error,
}

fn recorded(output: &[&str]) -> Recorded {
    if output.first().is_some_and(|l| l.trim() == "No results.") {
        return Recorded::Rows(Observation::default());
    }
    let lines: Vec<&str> = output
        .iter()
        .copied()
        .filter(|l| l.starts_with('│'))
        .collect();
    let count = output
        .iter()
        .find_map(|l| l.strip_suffix(" rows").or_else(|| l.strip_suffix(" row")))
        .and_then(|n| n.trim().parse::<usize>().ok());
    let (Some(count), Some(header)) = (count, lines.first()) else {
        return Recorded::Error;
    };
    let width = header.matches('│').count();
    let rows: Option<Vec<Row>> = lines[1..]
        .iter()
        .map(|line| {
            let inner = line.trim().trim_matches('│');
            let readable = line.matches('│').count() == width && !inner.contains('…');
            readable.then(|| inner.split('│').map(cell).collect())
        })
        .collect();
    match rows {
        Some(rows) if rows.len() == count => Recorded::Rows(Observation::from_rows(rows)),
        _ => Recorded::Unreadable,
    }
}

fn cell(text: &str) -> Cell {
    let text = text.trim();
    if let Ok(n) = text.parse::<i64>() {
        return Cell::Int(n);
    }
    match text {
        "true" => Cell::Bool(true),
        "false" => Cell::Bool(false),
        _ if text.starts_with('"') => serde_json::from_str::<String>(text)
            .map_or_else(|_| Cell::Other(text.into()), Cell::Str),
        _ => Cell::Other(text.into()),
    }
}

fn transcripts() -> Vec<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/iql");
    let mut paths: Vec<PathBuf> = CATEGORIES
        .iter()
        .flat_map(|category| {
            std::fs::read_dir(root.join(category)).expect("snapshot category exists")
        })
        .map(|entry| entry.expect("readable snapshot dir").path())
        .filter(|path| path.to_string_lossy().ends_with(".iql.out"))
        .collect();
    paths.sort();
    paths
}

fn check(path: &Path) -> Result<Report, String> {
    if SLOW_CASES.iter().any(|slow| path.ends_with(slow)) {
        return Err("too slow for the oracle's per-case budget: covered by snapshot tests".into());
    }
    let transcript = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let case = load(&transcript)?;
    let expected = case.expected;
    let factory = |queries: &[String]| {
        let mut adapters = crate::adapters(queries);
        adapters.push(Box::new(SpecAdapter {
            expected: expected.clone(),
        }));
        adapters
    };
    Ok(oracle::run(&case.history, &factory))
}

/// Run every case; fail listing every divergence.
pub fn check_all() {
    let paths = transcripts();
    let workers = std::thread::available_parallelism().map_or(4, |n| n.get().min(8));
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<(PathBuf, Result<Report, String>)>> = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                while let Some(path) = paths.get(next.fetch_add(1, Ordering::Relaxed)) {
                    let result = check(path);
                    results
                        .lock()
                        .expect("no worker panics holding the lock")
                        .push((path.clone(), result));
                }
            });
        }
    });
    let mut results = results.into_inner().expect("workers finished");
    results.sort_by(|a, b| a.0.cmp(&b.0));

    let mut failures = Vec::new();
    let mut unsupported = BTreeMap::<String, usize>::new();
    let mut compared = BTreeMap::<String, usize>::new();
    let mut reference_skips = BTreeMap::<String, usize>::new();
    for (path, result) in &results {
        let name = path
            .strip_prefix(env!("CARGO_MANIFEST_DIR"))
            .unwrap_or(path)
            .display();
        match result {
            Err(reason) => {
                *unsupported
                    .entry(reason.split(':').next().unwrap_or("").to_string())
                    .or_default() += 1;
            }
            Ok(report) => {
                for (adapter, n) in &report.compared {
                    *compared.entry(adapter.clone()).or_default() += n;
                }
                for skip in report.skips_of("reference") {
                    let reason: String = skip.reason.chars().take(70).collect();
                    *reference_skips.entry(reason).or_default() += 1;
                }
                failures.extend(report.divergences.iter().map(|d| format!("{name}: {d}")));
            }
        }
    }
    let run = results.iter().filter(|(_, r)| r.is_ok()).count();
    println!(
        "corpus: {run} of {} cases run; compared {compared:?}",
        results.len()
    );
    println!("unsupported cases by reason: {unsupported:#?}");
    println!("reference skips by reason: {reference_skips:#?}");
    assert!(
        failures.is_empty(),
        "{} divergences:\n{}",
        failures.len(),
        failures.join("\n")
    );
    assert!(
        run * 2 > results.len(),
        "fewer than half the corpus cases could run"
    );
    assert!(
        compared.get("spec").copied().unwrap_or(0) > 0,
        "no spec rows compared"
    );
}
