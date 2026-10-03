//! Programs that change rules and schemas commit them in the same transaction
//! as their facts: one WAL record, one snapshot with the new rules and data, or
//! (on any failure) nothing at all, now and after a restart. A crash between the
//! WAL record and the catalog file save is repaired by replay.

#![allow(clippy::unwrap_used)]

use super::*;
use crate::config::Config;
use crate::schema::{ColumnSchema, SchemaType};
use crate::storage::persist::wal::WalFault;
use crate::value::Value;
use tempfile::TempDir;

const KG: &str = "default";

fn open(temp: &TempDir) -> StorageEngine {
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.num_threads = 2;
    config.storage.persist.durability_mode = crate::config::DurabilityMode::Immediate;
    StorageEngine::new(config).unwrap()
}

fn wal_records(temp: &TempDir) -> Vec<String> {
    fs::read_to_string(temp.path().join("persist/wal/current.wal"))
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

fn int(v: i64) -> Tuple {
    Tuple::new(vec![Value::Int64(v)])
}

fn rows(storage: &StorageEngine, relation: &str) -> Vec<Tuple> {
    let mut rows: Vec<Tuple> = storage
        .get_snapshot_for(KG)
        .unwrap()
        .input_tuples
        .get(relation)
        .map(|ts| ts.iter().cloned().collect())
        .unwrap_or_default();
    rows.sort();
    rows
}

fn query(storage: &StorageEngine, program: &str) -> Vec<Tuple> {
    let mut rows = storage
        .execute_query_with_rules_tuples_on(KG, program)
        .unwrap();
    rows.sort();
    rows
}

fn rule(text: &str) -> CatalogChange {
    CatalogChange::RegisterRule(crate::statement::parse_rule_definition(text).unwrap())
}

fn int_schema(relation: &str) -> RelationSchema {
    RelationSchema::new(relation).with_column(ColumnSchema::new("x", SchemaType::Int))
}

fn insert(relation: &str, tuples: Vec<Tuple>) -> StagedChanges {
    StagedChanges::Facts(vec![FactChange::Insert {
        relation: relation.to_string(),
        tuples,
    }])
}

fn delete(relation: &str, tuples: Vec<Tuple>) -> StagedChanges {
    StagedChanges::Facts(vec![FactChange::Delete {
        relation: relation.to_string(),
        tuples,
    }])
}

fn program(statements: Vec<StagedChanges>) -> WriteProgram {
    let mut program = WriteProgram::new();
    for (index, changes) in statements.into_iter().enumerate() {
        program.push(index, changes);
    }
    program
}

fn catalog(change: CatalogChange) -> StagedChanges {
    StagedChanges::Catalog(change)
}

/// A migration: a schema, data checked against it, and a rule over the data.
fn migration() -> WriteProgram {
    program(vec![
        catalog(CatalogChange::DefineSchema(int_schema("item"))),
        insert("item", vec![int(1), int(2)]),
        catalog(rule("big(X) <- item(X), X > 1")),
    ])
}

fn catalog_files(temp: &TempDir) -> (Option<String>, Option<String>) {
    let kg_dir = temp.path().join(KG);
    (
        fs::read_to_string(kg_dir.join("rules/catalog.json")).ok(),
        fs::read_to_string(kg_dir.join("schema.json")).ok(),
    )
}

#[test]
fn migration_commits_schema_data_and_rule_as_one_record() {
    let temp = TempDir::new().unwrap();
    {
        let storage = open(&temp);
        let before = wal_records(&temp).len();
        let commit = storage.commit_program(KG, migration(), None).unwrap();
        assert_eq!(wal_records(&temp).len(), before + 1);
        assert_eq!(
            commit
                .statements
                .iter()
                .map(|s| s.effect.clone())
                .collect::<Vec<_>>(),
            [
                StatementEffect::Catalog(CatalogOutcome::SchemaDefined),
                StatementEffect::Facts(FactCount {
                    inserted: 2,
                    deleted: 0
                }),
                StatementEffect::Catalog(CatalogOutcome::RuleRegistered(
                    crate::rule_catalog::RuleRegisterResult::Created
                )),
            ]
        );
        assert_eq!(query(&storage, "q(X) <- big(X)"), [int(2)]);
    }
    let storage = open(&temp);
    assert!(storage.has_schema_in(KG, "item").unwrap());
    assert_eq!(query(&storage, "q(X) <- big(X)"), [int(2)]);
}

#[test]
fn late_failure_leaves_data_schema_and_rules_unchanged_now_and_after_restart() {
    let temp = TempDir::new().unwrap();
    {
        let storage = open(&temp);
        storage
            .register_rule_in(
                KG,
                &crate::statement::parse_rule_definition("pair(X, Y) <- item(X), item(Y)").unwrap(),
            )
            .unwrap();
        let files = catalog_files(&temp);
        let records = wal_records(&temp);
        let version = storage.get_snapshot_for(KG).unwrap().version;

        let mut failing = migration();
        // Arity mismatch with the existing two-column rule `pair`.
        failing.push(3, catalog(rule("pair(X) <- item(X)")));
        let err = storage.commit_program(KG, failing, None).unwrap_err();
        let CommitError::Rejected { statement, error } = err else {
            panic!("expected a rejection, got {err:?}");
        };
        assert_eq!(statement, 3);
        assert!(error.to_string().contains("Arity mismatch"), "{error}");

        assert_eq!(storage.get_snapshot_for(KG).unwrap().version, version);
        assert_eq!(wal_records(&temp), records);
        assert_eq!(catalog_files(&temp), files);
        assert!(!storage.has_schema_in(KG, "item").unwrap());
        assert!(rows(&storage, "item").is_empty());
        assert_eq!(storage.list_rules_in(KG).unwrap(), ["pair"]);
    }
    let storage = open(&temp);
    assert!(!storage.has_schema_in(KG, "item").unwrap());
    assert!(rows(&storage, "item").is_empty());
    assert_eq!(storage.list_rules_in(KG).unwrap(), ["pair"]);
}

#[test]
fn facts_validate_against_catalog_changes_staged_before_them() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    let text = || Tuple::new(vec![Value::string("x")]);

    // A schema declared earlier in the program rejects a later insert.
    let err = storage
        .commit_program(
            KG,
            program(vec![
                catalog(CatalogChange::DefineSchema(int_schema("n"))),
                insert("n", vec![text()]),
            ]),
            None,
        )
        .unwrap_err();
    assert!(
        matches!(err, CommitError::Rejected { statement: 1, .. }),
        "{err:?}"
    );
    assert!(!storage.has_schema_in(KG, "n").unwrap());

    // A rule registered earlier makes its head a view; dropping it frees the name.
    let err = storage
        .commit_program(
            KG,
            program(vec![
                catalog(rule("v(X) <- n(X)")),
                insert("v", vec![int(1)]),
            ]),
            None,
        )
        .unwrap_err();
    assert!(
        matches!(err, CommitError::Rejected { statement: 1, .. }),
        "{err:?}"
    );
    storage
        .register_rule_in(
            KG,
            &crate::statement::parse_rule_definition("v(X) <- n(X)").unwrap(),
        )
        .unwrap();
    storage
        .commit_program(
            KG,
            program(vec![
                catalog(CatalogChange::DropRule("v".into())),
                insert("v", vec![int(1)]),
            ]),
            None,
        )
        .unwrap();
    assert!(storage.list_rules_in(KG).unwrap().is_empty());
    assert_eq!(rows(&storage, "v"), [int(1)]);
}

#[test]
fn rule_arity_changes_only_with_the_old_rule_dropped_first() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage
        .insert_tuples_into(KG, "e", vec![Tuple::from_pair(1, 2)])
        .unwrap();
    storage
        .register_rule_in(
            KG,
            &crate::statement::parse_rule_definition("p(X, Y) <- e(X, Y)").unwrap(),
        )
        .unwrap();

    let err = storage
        .commit_program(KG, program(vec![catalog(rule("p(X) <- e(X, Y)"))]), None)
        .unwrap_err();
    assert!(
        matches!(err, CommitError::Rejected { statement: 0, .. }),
        "{err:?}"
    );
    assert_eq!(query(&storage, "q(X, Y) <- p(X, Y)").len(), 1);

    storage
        .commit_program(
            KG,
            program(vec![
                catalog(CatalogChange::DropRule("p".into())),
                catalog(rule("p(X) <- e(X, Y)")),
            ]),
            None,
        )
        .unwrap();
    assert_eq!(storage.rule_arity_in(KG, "p").unwrap(), Some(1));
    assert_eq!(query(&storage, "q(X) <- p(X)"), [int(1)]);
}

#[test]
fn crash_before_the_catalog_save_is_repaired_from_the_wal() {
    let temp = TempDir::new().unwrap();
    let before = {
        let storage = open(&temp);
        storage
            .insert_tuples_into(KG, "seed", vec![int(0)])
            .unwrap();
        catalog_files(&temp)
    };
    {
        let storage = open(&temp);
        storage.commit_program(KG, migration(), None).unwrap();
    }
    // Simulate a crash after the WAL record but before the catalog files
    // were saved: put back the files from before the commit.
    let kg_dir = temp.path().join(KG);
    match before {
        (Some(rules), _) => fs::write(kg_dir.join("rules/catalog.json"), rules).unwrap(),
        (None, _) => {
            let _ = fs::remove_file(kg_dir.join("rules/catalog.json"));
        }
    }
    let _ = fs::remove_file(kg_dir.join("schema.json"));

    let storage = open(&temp);
    assert!(storage.has_schema_in(KG, "item").unwrap());
    assert_eq!(query(&storage, "q(X) <- big(X)"), [int(2)]);
    // Replay saved the files and released the WAL's copy.
    drop(storage);
    let storage = open(&temp);
    assert_eq!(query(&storage, "q(X) <- big(X)"), [int(2)]);
}

#[test]
fn replay_applies_catalog_changes_in_commit_order() {
    let temp = TempDir::new().unwrap();
    {
        let storage = open(&temp);
        // Facts keep every record in the WAL (they are not flushed yet).
        storage
            .insert_tuples_into(KG, "e", vec![Tuple::from_pair(1, 2)])
            .unwrap();
        let p = |text| {
            program(vec![
                catalog(rule(text)),
                insert("e", vec![Tuple::from_pair(3, 4)]),
            ])
        };
        storage
            .commit_program(KG, p("p(X) <- e(X, Y)"), None)
            .unwrap();
        storage
            .commit_program(
                KG,
                program(vec![catalog(CatalogChange::DropRule("p".into()))]),
                None,
            )
            .unwrap();
        storage
            .commit_program(KG, p("r(Y) <- e(X, Y)"), None)
            .unwrap();
    }
    let _ = fs::remove_file(temp.path().join(KG).join("rules/catalog.json"));
    let storage = open(&temp);
    assert_eq!(storage.list_rules_in(KG).unwrap(), ["r"]);
}

#[test]
fn saved_catalog_changes_leave_the_wal() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage
        .commit_program(
            KG,
            program(vec![
                catalog(CatalogChange::DefineSchema(int_schema("item"))),
                catalog(rule("v(X) <- item(X)")),
            ]),
            None,
        )
        .unwrap();
    assert!(wal_records(&temp).is_empty(), "{:?}", wal_records(&temp));
}

#[test]
fn a_new_kg_of_a_dropped_name_does_not_replay_its_rules() {
    let temp = TempDir::new().unwrap();
    {
        let storage = open(&temp);
        storage.create_knowledge_graph("k").unwrap();
        storage
            .commit_program(
                "k",
                program(vec![
                    catalog(rule("p(X) <- e(X)")),
                    insert("e", vec![int(1)]),
                ]),
                None,
            )
            .unwrap();
        storage.drop_knowledge_graph("k").unwrap();
        storage.create_knowledge_graph("k").unwrap();
        assert!(storage.list_rules_in("k").unwrap().is_empty());
    }
    let storage = open(&temp);
    assert!(storage.list_rules_in("k").unwrap().is_empty());
}

#[test]
fn persistence_failure_leaves_catalogs_unchanged_now_and_after_restart() {
    for fault in [WalFault::Write, WalFault::Sync] {
        let temp = TempDir::new().unwrap();
        {
            let storage = open(&temp);
            let files = catalog_files(&temp);
            storage.persist.inject_wal_fault(fault);
            let err = storage.commit_program(KG, migration(), None).unwrap_err();
            assert!(matches!(err, CommitError::Failed(_)), "{fault:?}: {err:?}");
            assert_eq!(catalog_files(&temp), files, "{fault:?}");
            assert!(!storage.has_schema_in(KG, "item").unwrap(), "{fault:?}");
            assert!(storage.list_rules_in(KG).unwrap().is_empty(), "{fault:?}");
        }
        let storage = open(&temp);
        assert!(!storage.has_schema_in(KG, "item").unwrap(), "{fault:?}");
        assert!(storage.list_rules_in(KG).unwrap().is_empty(), "{fault:?}");
        assert!(rows(&storage, "item").is_empty(), "{fault:?}");
    }
}

#[test]
fn session_schema_of_a_failed_program_is_not_registered() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    let err = storage
        .commit_program(
            KG,
            program(vec![
                catalog(CatalogChange::DefineSessionSchema(int_schema("s"))),
                catalog(CatalogChange::DropRule("missing".into())),
            ]),
            None,
        )
        .unwrap_err();
    assert!(
        matches!(err, CommitError::Rejected { statement: 1, .. }),
        "{err:?}"
    );
    assert!(!storage.has_schema_in(KG, "s").unwrap());
}

#[test]
fn readers_see_one_rule_generation_with_its_data() {
    let temp = TempDir::new().unwrap();
    let storage = Arc::new(open(&temp));
    storage
        .insert_tuples_into(KG, "version", vec![int(0)])
        .unwrap();
    storage
        .register_rule_in(
            KG,
            &crate::statement::parse_rule_definition("gen(X) <- version(X), X = 0").unwrap(),
        )
        .unwrap();
    let done = Arc::new(AtomicBool::new(false));

    // Generation g has data version(g) and rule gen(X) <- version(X), X = g:
    // a reader must always see gen hold exactly the current version.
    let reader = {
        let storage = Arc::clone(&storage);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let mut reads = 0;
            while !done.load(Ordering::Relaxed) {
                let rows = storage
                    .execute_query_with_rules_tuples_on(KG, "q(X) <- gen(X)")
                    .unwrap();
                assert_eq!(
                    rows.len(),
                    1,
                    "mixed generation after {reads} reads: {rows:?}"
                );
                reads += 1;
            }
        })
    };
    for g in 1..=50 {
        let next = format!("gen(X) <- version(X), X = {g}");
        storage
            .commit_program(
                KG,
                program(vec![
                    delete("version", vec![int(g - 1)]),
                    catalog(CatalogChange::DropRule("gen".into())),
                    insert("version", vec![int(g)]),
                    catalog(rule(&next)),
                ]),
                None,
            )
            .unwrap();
    }
    done.store(true, Ordering::Relaxed);
    reader.join().unwrap();
    assert_eq!(query(&storage, "q(X) <- gen(X)"), [int(50)]);
}
