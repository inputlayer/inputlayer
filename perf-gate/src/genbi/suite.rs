//! Reading the genbi-trust suite in place (read-only; nothing is copied).
//!
//! Public per-scenario files define what an agent is asked and what changes:
//! `inputlayer-seed.iql`, `queries.json`, `mutations.json`. The
//! evaluator-private `expected.json` is read only for scoring and its rows
//! never leave this process: [`Expected`] does not print them.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;

/// The suite checkout, e.g. `$GENBI_TRUST_DIR`.
pub struct Suite {
    pub dir: PathBuf,
    pub version: String,
    pub cases: Vec<Case>,
    categories: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Case {
    pub id: String,
    pub category: String,
}

#[derive(Deserialize)]
struct SuiteFile {
    version: Value,
    scenarios: Vec<Case>,
    categories: Vec<CategoryFile>,
}

#[derive(Deserialize)]
struct CategoryFile {
    id: String,
    name: String,
}

/// A business question: a named check and its SQL over the reference model.
#[derive(Debug, Clone, Deserialize)]
pub struct Question {
    pub name: String,
    pub sql: String,
}

/// One ordered change and the questions checked after it.
#[derive(Debug, Clone, Deserialize)]
pub struct Mutation {
    pub label: String,
    pub sql: String,
    #[serde(default)]
    pub queries: Vec<Question>,
}

#[derive(Deserialize)]
struct QueriesFile {
    queries: Vec<Question>,
}

/// One scenario's public inputs.
pub struct Scenario {
    pub seed: Seed,
    /// Questions checked in the `initial` phase.
    pub questions: Vec<Question>,
    pub mutations: Vec<Mutation>,
}

/// The executable statements of `inputlayer-seed.iql`.
///
/// Its `.kg` commands are dropped (the harness owns the knowledge graph) and
/// so are its `?` queries (the agent's questions come from `queries.json`).
pub struct Seed {
    pub statements: Vec<String>,
    /// Declared relation schemas: relation -> column names.
    pub schemas: BTreeMap<String, Vec<String>>,
    /// Relations defined by a rule head.
    pub rules: BTreeSet<String>,
}

/// Evaluator-private checkpoints. `Debug` shows shape only, never rows.
pub struct Expected {
    pub phases: Vec<ExpectedPhase>,
}

#[derive(Deserialize)]
pub struct ExpectedPhase {
    pub phase: String,
    pub checks: Vec<ExpectedCheck>,
}

#[derive(Deserialize)]
pub struct ExpectedCheck {
    pub name: String,
    pub expected_rows: Vec<Vec<Value>>,
}

#[derive(Deserialize)]
struct ExpectedFile {
    phases: Vec<ExpectedPhase>,
}

impl fmt::Debug for Expected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let checks: usize = self.phases.iter().map(|p| p.checks.len()).sum();
        write!(f, "Expected({} phases, {checks} checks)", self.phases.len())
    }
}

/// Phase name of the checks taken before any mutation.
pub const INITIAL_PHASE: &str = "initial";

impl Suite {
    pub fn open(dir: &Path) -> Result<Self> {
        let path = dir.join("generated/scenario-suite.json");
        let file: SuiteFile = read_json(&path)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            version: match file.version {
                Value::String(v) => v,
                other => other.to_string(),
            },
            cases: file.scenarios,
            categories: file
                .categories
                .into_iter()
                .map(|c| (c.id, c.name))
                .collect(),
        })
    }

    pub fn category_name(&self, id: &str) -> &str {
        self.categories.get(id).map_or("", String::as_str)
    }

    /// The first case of every category, in suite order.
    pub fn representative(&self) -> Vec<Case> {
        let mut seen = BTreeSet::new();
        self.cases
            .iter()
            .filter(|c| seen.insert(c.category.clone()))
            .cloned()
            .collect()
    }

    pub fn scenario(&self, case: &Case) -> Result<Scenario> {
        let dir = self.dir.join("generated/scenarios").join(&case.id);
        let seed_path = dir.join("inputlayer-seed.iql");
        let seed = std::fs::read_to_string(&seed_path)
            .with_context(|| format!("read {}", seed_path.display()))?;
        let queries: QueriesFile = read_json(&dir.join("queries.json"))?;
        Ok(Scenario {
            seed: Seed::parse(&seed),
            questions: queries.queries,
            mutations: read_json(&dir.join("mutations.json"))?,
        })
    }

    pub fn expected(&self, case: &Case) -> Result<Expected> {
        let path = self
            .dir
            .join("generated/private/scenarios")
            .join(&case.id)
            .join("expected.json");
        let file: ExpectedFile = read_json(&path)?;
        Ok(Expected {
            phases: file.phases,
        })
    }
}

impl Seed {
    pub fn parse(text: &str) -> Self {
        let mut seed = Seed {
            statements: Vec::new(),
            schemas: BTreeMap::new(),
            rules: BTreeSet::new(),
        };
        for line in text.lines().map(str::trim) {
            if !line.starts_with('+') {
                continue;
            }
            if let Some((head, _)) = line.split_once("<-") {
                if let Some(name) = relation_name(head) {
                    seed.rules.insert(name.to_string());
                }
            } else if let Some((name, columns)) = schema(line) {
                seed.schemas.insert(name, columns);
            }
            seed.statements.push(line.to_string());
        }
        seed
    }

    /// The statements to send, with consecutive bulk inserts into one
    /// relation (`+rel[(...)]` lines) merged into a single batch: the same
    /// facts, loaded the way an ingestion pipeline would. Each batch keeps
    /// the index of its first seed statement for attribution.
    pub fn batches(&self) -> Vec<(usize, String)> {
        let mut batches: Vec<(usize, String)> = Vec::new();
        let mut open: Option<String> = None;
        for (index, statement) in self.statements.iter().enumerate() {
            let bulk = relation_name(statement).and_then(|name| {
                let tuples = statement
                    .strip_prefix('+')?
                    .strip_prefix(name)?
                    .strip_prefix('[')?
                    .strip_suffix(']')?;
                Some((name, tuples))
            });
            match (bulk, &open, batches.last_mut()) {
                (Some((name, tuples)), Some(current), Some((_, batch))) if current == name => {
                    batch.truncate(batch.len() - 1);
                    batch.push_str(", ");
                    batch.push_str(tuples);
                    batch.push(']');
                }
                _ => {
                    open = bulk.map(|(name, _)| name.to_string());
                    batches.push((index, statement.clone()));
                }
            }
        }
        batches
    }

    /// Whether the seed declares or derives `relation`.
    pub fn defines(&self, relation: &str) -> bool {
        self.schemas.contains_key(relation) || self.rules.contains(relation)
    }
}

/// `+name(...` -> `name`.
fn relation_name(statement: &str) -> Option<&str> {
    let rest = statement.trim().strip_prefix('+')?;
    let end = rest.find(|c: char| !(c.is_alphanumeric() || c == '_'))?;
    Some(&rest[..end]).filter(|n| !n.is_empty())
}

/// `+name(col: type, ...)` -> (`name`, columns).
fn schema(line: &str) -> Option<(String, Vec<String>)> {
    let name = relation_name(line)?;
    let body = line
        .strip_prefix('+')?
        .strip_prefix(name)?
        .strip_prefix('(')?
        .strip_suffix(')')?;
    let mut columns = Vec::new();
    for part in body.split(',') {
        let (column, kind) = part.split_once(':')?;
        let (column, kind) = (column.trim(), kind.trim());
        let is_word = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_alphanumeric() || c == '_');
        if !is_word(column) || !is_word(kind) {
            return None;
        }
        columns.push(column.to_string());
    }
    Some((name.to_string(), columns))
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_keeps_writes_and_learns_schemas_and_rules() {
        let seed = Seed::parse(
            ".kg create bench_x\n.kg use bench_x\n// note\n\
             +edges(tenant: string,parent: string,child: string)\n\
             +edges[(\"t1\",\"a: b\",\"c\")]\n\
             +reach(T,A,C) <- reach(T,A,B), edges(T,B,C)\n\
             ?reach(T,A,C)\n",
        );
        assert_eq!(seed.statements.len(), 3);
        assert_eq!(seed.schemas["edges"], ["tenant", "parent", "child"]);
        assert!(seed.rules.contains("reach"));
        assert!(seed.defines("edges") && seed.defines("reach") && !seed.defines("clock"));
    }

    #[test]
    fn consecutive_bulk_inserts_share_a_batch() {
        let seed =
            Seed::parse("+a(x: int)\n+a[(1)]\n+a[(2), (3)]\n+b[(\"]\")]\n+a[(4)]\n+r(X) <- a(X)\n");
        assert_eq!(
            seed.batches(),
            [
                (0, "+a(x: int)".to_string()),
                (1, "+a[(1), (2), (3)]".to_string()),
                (3, "+b[(\"]\")]".to_string()),
                (4, "+a[(4)]".to_string()),
                (5, "+r(X) <- a(X)".to_string()),
            ]
        );
    }

    #[test]
    fn expected_debug_hides_rows() {
        let expected = Expected {
            phases: vec![ExpectedPhase {
                phase: INITIAL_PHASE.into(),
                checks: vec![ExpectedCheck {
                    name: "c".into(),
                    expected_rows: vec![vec![Value::from("secret")]],
                }],
            }],
        };
        let shown = format!("{expected:?}");
        assert!(!shown.contains("secret"), "{shown}");
    }
}
