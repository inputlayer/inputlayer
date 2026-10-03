//! A program's fact, rule and schema changes commit as one transaction
//! (issue #170): a failed migration or pack installation leaves data, schemas
//! and rules unchanged, readers and subscribers see one rule generation, and a
//! command that cannot join the transaction fails the program before any write.

use inputlayer::protocol::handler::Notification;
use inputlayer::protocol::{ErrorCode, Handler, ProgramError, QueryResult, WireValue};
use inputlayer::{Config, StorageEngine};
use std::path::Path;
use tempfile::TempDir;

const KG: &str = "default";

fn handler(dir: &Path) -> Handler {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    config.storage.persist.durability_mode = inputlayer::config::DurabilityMode::Immediate;
    Handler::new(StorageEngine::new(config).expect("storage"))
}

async fn run(handler: &Handler, program: &str) -> Result<QueryResult, ProgramError> {
    handler
        .execute_program_status(
            None,
            Some(KG.to_string()),
            program.to_string(),
            None,
            &handler.request_control(None),
        )
        .await
}

/// Rows of `query`, rendered and sorted.
async fn rows(handler: &Handler, query: &str) -> Vec<String> {
    let result = run(handler, query).await.expect(query);
    let mut rows: Vec<String> = result
        .rows
        .iter()
        .map(|row| format!("{:?}", row.values))
        .collect();
    rows.sort();
    rows
}

fn messages(result: &QueryResult) -> Vec<String> {
    result
        .rows
        .iter()
        .map(|row| match row.values.as_slice() {
            [WireValue::String(message)] => message.clone(),
            other => format!("{other:?}"),
        })
        .collect()
}

fn errors(result: &QueryResult) -> Vec<(usize, ErrorCode)> {
    result.errors.iter().map(|e| (e.index, e.code)).collect()
}

/// What a program could have changed: schemas, rules, and the data.
async fn state(handler: &Handler) -> (Vec<String>, Vec<String>, Vec<String>) {
    let (schemas, rules) = {
        let storage = handler.get_storage();
        let mut schemas = storage.list_schemas_in(KG).expect("schemas");
        schemas.sort();
        (schemas, storage.list_rules_in(KG).expect("rules"))
    };
    (schemas, rules, rows(handler, "?person(N, A)").await)
}

const MIGRATION: &str = "+person(name: string, age: int)\n\
                         +person[(\"ann\", 30), (\"bo\", 12)]\n\
                         +adult(N) <- person(N, A), A >= 18";

#[tokio::test]
async fn failed_migration_leaves_data_schema_and_rules_unchanged() {
    let dir = TempDir::new().unwrap();
    let before = {
        let handler = handler(dir.path());
        run(&handler, "+legacy(1)\n+old(X) <- legacy(X)")
            .await
            .unwrap();
        let before = state(&handler).await;

        // The last statement violates the schema the migration declares.
        let program = format!("{MIGRATION}\n.rule drop old\n+person[(\"cy\", \"unknown\")]");
        let result = run(&handler, &program).await.unwrap();
        assert_eq!(errors(&result), [(4, ErrorCode::Validation)]);
        assert!(
            result.errors[0].message.ends_with(
                "(rolled back: none of the program's 5 write statements 0-4 was applied)"
            ),
            "{:?}",
            result.errors
        );
        assert_eq!(messages(&result).len(), 1, "{result:?}");
        assert_eq!(state(&handler).await, before);
        before
    };
    // Nothing reached the WAL either.
    let handler = handler(dir.path());
    assert_eq!(state(&handler).await, before);
    assert_eq!(rows(&handler, "?old(X)").await.len(), 1);
}

#[tokio::test]
async fn migration_commits_schema_data_and_rules_together() {
    let dir = TempDir::new().unwrap();
    {
        let handler = handler(dir.path());
        let result = run(&handler, &format!("{MIGRATION}\n?adult(N)"))
            .await
            .unwrap();
        assert!(result.errors.is_empty(), "{:?}", result.errors);
        assert_eq!(messages(&result), ["ann"]);
    }
    let handler = handler(dir.path());
    let (schemas, rules, people) = state(&handler).await;
    assert_eq!(
        (schemas, rules),
        (vec!["person".into()], vec!["adult".into()])
    );
    assert_eq!(people.len(), 2);
}

#[tokio::test]
async fn rule_and_data_replacement_notifies_after_one_commit() {
    let dir = TempDir::new().unwrap();
    let handler = handler(dir.path());
    run(&handler, "+v(1)\n+cur(X) <- v(X), X = 1")
        .await
        .unwrap();
    let mut notifications = handler.subscribe_notifications();

    let result = run(
        &handler,
        "-v(1)\n.rule drop cur\n+v(2)\n+cur(X) <- v(X), X = 2",
    )
    .await
    .unwrap();
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    let mut seen = Vec::new();
    while let Ok(notification) = notifications.try_recv() {
        match notification {
            Notification::RuleChange {
                rule_name,
                operation,
                ..
            } => seen.push(format!("rule {rule_name} {operation}")),
            Notification::PersistentUpdate {
                relation,
                operation,
                ..
            } => seen.push(format!("facts {relation} {operation}")),
            _ => {}
        }
    }
    assert_eq!(
        seen,
        ["rule cur dropped", "rule cur registered", "facts v update"]
    );
    assert_eq!(rows(&handler, "?cur(X)").await, ["[Int64(2)]"]);

    // A program that fails late notifies nobody.
    let result = run(&handler, "-v(2)\n.rule drop cur\n.rule drop missing")
        .await
        .unwrap();
    assert_eq!(errors(&result), [(2, ErrorCode::NotFound)]);
    assert!(notifications.try_recv().is_err());
    assert_eq!(rows(&handler, "?cur(X)").await, ["[Int64(2)]"]);
}

#[tokio::test]
async fn statement_reading_the_kg_after_a_rule_change_uses_the_new_rule() {
    let dir = TempDir::new().unwrap();
    let handler = handler(dir.path());
    run(&handler, "+e(1, 2)\n+e(3, 4)\n+keep(X) <- e(X, Y)")
        .await
        .unwrap();
    // Redefine `keep` and delete everything the new definition selects.
    let result = run(
        &handler,
        ".rule drop keep\n+keep(X) <- e(X, Y), X > 2\n-e(X, Y) <- keep(X), e(X, Y)",
    )
    .await
    .unwrap();
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    assert_eq!(rows(&handler, "?e(X, Y)").await, ["[Int64(1), Int64(2)]"]);
}

#[tokio::test]
async fn command_outside_the_transaction_is_rejected_before_any_write() {
    let dir = TempDir::new().unwrap();
    let handler = handler(dir.path());
    handler
        .get_storage()
        .create_knowledge_graph("other")
        .expect("create");
    for (program, index) in [
        ("+a(1)\n.kg use other\n+b(1)", 1),
        ("+r(x: int)\n.index list", 1),
        (".compact\n+a(1)", 0),
        ("+a(1)\n.rel", 1),
    ] {
        let result = run(&handler, program).await.unwrap();
        assert_eq!(
            errors(&result),
            [(index, ErrorCode::Unsupported)],
            "{program}"
        );
        assert_eq!(
            rows(&handler, "?a(X)").await,
            Vec::<String>::new(),
            "{program}"
        );
    }
    assert!(handler
        .get_storage()
        .list_schemas_in(KG)
        .unwrap()
        .is_empty());
}

/// A local registry holding one pack, `mini`, whose rules file is `rules`.
fn registry(root: &Path, rules: &str) {
    let pack = root.join("ontologies").join("mini");
    std::fs::create_dir_all(pack.join("rules")).expect("create pack");
    std::fs::write(
        pack.join("ontology.toml"),
        "[ontology]\nname = \"mini\"\nversion = \"0.1.0\"\nrules = [\"rules/r.iql\"]\n",
    )
    .expect("write manifest");
    std::fs::write(pack.join("rules/r.iql"), rules).expect("write rules");
}

#[tokio::test]
async fn failed_pack_installation_applies_nothing() {
    let dir = TempDir::new().unwrap();
    let packs = TempDir::new().unwrap();
    // The only test in this binary that installs packs, so the variable is ours.
    std::env::set_var("INPUTLAYER_REGISTRY", packs.path());
    let handler = handler(dir.path());

    registry(
        packs.path(),
        &format!("{MIGRATION}\n+person[(\"cy\", \"unknown\")]\n"),
    );
    let before = state(&handler).await;
    let err = run(&handler, ".ontology install mini").await.unwrap_err();
    assert!(err.message.contains("nothing was applied"), "{err:?}");
    assert_eq!(state(&handler).await, before);
    assert!(rows(&handler, "?pack_meta(N, V, D)").await.is_empty());

    registry(packs.path(), &format!("{MIGRATION}\n"));
    run(&handler, ".ontology install mini")
        .await
        .expect("install");
    assert_eq!(rows(&handler, "?adult(N)").await, ["[String(\"ann\")]"]);
    assert_eq!(rows(&handler, "?pack_meta(N, V, D)").await.len(), 1);
}
