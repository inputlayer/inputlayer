//! Offline backup round trip: a data directory restored from a backup
//! serves exactly what the original served - KGs, facts, schemas, rules,
//! users and ACLs - through the real handler and the real CLI binary.

// Test setup aborts on failure; `unwrap` is the intended behavior.
#![allow(clippy::unwrap_used)]

use inputlayer::auth::Principal;
use inputlayer::config::DurabilityMode;
use inputlayer::protocol::Handler;
use inputlayer::storage::backup::{self, Manifest};
use inputlayer::storage::DataDirLock;
use inputlayer::{Config, StorageEngine};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;

/// Small flush threshold: some shards reach parquet batches, the rest stay
/// in the WAL, so the backup must carry both.
fn config(dir: &Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    config.storage.performance.num_threads = 2;
    config.storage.persist.durability_mode = DurabilityMode::Immediate;
    config.storage.persist.buffer_size = 3;
    config.http.auth.credentials_file = Some(dir.join("credentials.toml"));
    config.storage.backup_dir = Some(dir.with_file_name("backups"));
    config
}

fn open(dir: &Path) -> Handler {
    let handler = Handler::new(StorageEngine::new(config(dir)).unwrap());
    handler.bootstrap_auth();
    handler
}

/// An admin principal through a fresh API key; labels are unique per key.
fn admin(handler: &Handler) -> Principal {
    static KEYS: AtomicU64 = AtomicU64::new(0);
    let label = format!("backup-test-{}", KEYS.fetch_add(1, Ordering::Relaxed));
    let key = handler.create_api_key(&label, "admin", None).unwrap();
    handler.authenticate_api_key(&key).unwrap()
}

/// Per KG, the program that builds it: typed schema, facts, a delete,
/// recursive and negated persistent rules, a vector index.
const KGS: [(&str, &str); 3] = [
    (
        "graph",
        "+edge[(1, 2), (2, 3), (3, 4), (4, 5), (9, 9)]\n\
         -edge[(9, 9)]\n\
         +reach(X, Y) <- edge(X, Y)\n\
         +reach(X, Z) <- edge(X, Y), reach(Y, Z)\n\
         +node[(1,), (2,), (3,), (4,), (5,), (6,)]\n\
         +isolated(X) <- node(X), !edge(X, _), !edge(_, X)",
    ),
    (
        "hr",
        "+employee(id: int, name: string, dept: int)\n\
         +employee[(1, \"Alice\", 10), (2, \"Bob\", 10), (3, \"Chen\", 20)]\n\
         +dept[(10, \"Eng\"), (20, \"Sales\")]\n\
         +works_in(N, D) <- employee(_, N, I), dept(I, D)",
    ),
    (
        "vec",
        "+docs(id: int, title: string, emb: vector)\n\
         +docs[(1, \"x\", [1.0, 0.0, 0.0]), (2, \"y\", [0.0, 1.0, 0.0]), (3, \"xy\", [0.7, 0.7, 0.0])]\n\
         .index create doc_idx on docs(emb) metric cosine",
    ),
];

/// Queries whose answers must survive the round trip unchanged.
const QUERIES: [(&str, &str); 6] = [
    ("graph", "?reach(X, Y)"),
    ("graph", "?isolated(X)"),
    ("graph", "?edge(X, Y)"),
    ("hr", "?works_in(N, D)"),
    ("hr", "?employee(I, N, D)"),
    (
        "vec",
        "?hnsw_nearest(\"doc_idx\", [1.0, 0.1, 0.0], 2, Id, Dist)",
    ),
];

/// Build the representative state, then stop without a graceful shutdown,
/// as after `kill -9`: unflushed facts remain only in the WAL.
async fn populate(dir: &Path) {
    let handler = open(dir);
    let admin = admin(&handler);
    for (kg, program) in KGS {
        handler.get_storage().create_knowledge_graph(kg).unwrap();
        for statement in program.lines() {
            handler
                .execute_program(None, Some(kg.into()), statement.into(), Some(&admin))
                .await
                .unwrap();
        }
    }
    handler
        .handle_user_create("mallory", "password-m", "viewer")
        .unwrap();
    handler
        .handle_kg_acl_grant("graph", "mallory", "viewer")
        .unwrap();
}

/// Everything a client can observe, as comparable text.
async fn observe(handler: &Handler) -> Vec<String> {
    let admin = admin(handler);
    let mut seen = Vec::new();
    for (kg, query) in QUERIES {
        let result = handler
            .execute_program(None, Some(kg.into()), query.into(), Some(&admin))
            .await
            .unwrap();
        let mut rows: Vec<String> = result
            .rows
            .iter()
            .map(|r| serde_json::to_string(r).unwrap())
            .collect();
        rows.sort();
        seen.push(format!("{kg} {query} -> {}", rows.join(" ")));
    }
    let storage = handler.get_storage();
    for kg in ["graph", "hr", "vec"] {
        seen.push(format!(
            "{kg} rules {:?}",
            storage.list_rules_in(kg).unwrap()
        ));
        seen.push(format!(
            "{kg} acl {}",
            handler.handle_kg_acl_list(kg).unwrap()
        ));
    }
    seen.push(format!("kgs {:?}", storage.list_knowledge_graphs()));
    let users = handler.handle_user_list().unwrap();
    seen.push(format!(
        "users {}",
        serde_json::to_string(&users.rows).unwrap()
    ));
    seen
}

#[tokio::test]
async fn restored_directory_serves_exactly_what_the_original_served() {
    let temp = TempDir::new().unwrap();
    let (data, backup_dir, restored) = (
        temp.path().join("data"),
        temp.path().join("backup"),
        temp.path().join("restored"),
    );
    populate(&data).await;
    let expected = observe(&open(&data)).await;
    // The observation reopened the engine; take the backup from the
    // unflushed state again so the WAL path is covered.
    std::fs::remove_dir_all(&data).unwrap();
    populate(&data).await;

    backup::create(&data, &backup_dir).unwrap();

    let manifest = Manifest::load(&backup_dir).unwrap();
    let has =
        |pred: &dyn Fn(&str, u64) -> bool| manifest.files.iter().any(|f| pred(&f.path, f.size));
    assert!(
        has(&|p, s| p == "persist/wal/current.wal" && s > 0),
        "no WAL in backup"
    );
    assert!(
        has(&|p, _| p.starts_with("persist/batches/")),
        "no batch in backup"
    );
    assert!(has(&|p, _| p == "metadata/knowledge_graphs.json"));
    assert!(manifest.directories.contains(&"hr/relations".to_string()));
    assert!(
        !has(&|p, _| p == "LOCK"),
        "the source's LOCK must not be copied"
    );

    backup::verify(&backup_dir).unwrap();
    let report = backup::restore(&backup_dir, &restored).unwrap();
    assert_eq!(report.files, manifest.files.len());

    let handler = open(&restored);
    assert_eq!(observe(&handler).await, expected);

    // ACLs are enforced, not just listed: the restored viewer reads `graph`
    // with its restored password and is denied `hr`.
    let mallory = handler.authenticate_user("mallory", "password-m").unwrap();
    let read = |kg: &str| {
        handler.execute_program(None, Some(kg.into()), "?edge(X, Y)".into(), Some(&mallory))
    };
    assert_eq!(read("graph").await.unwrap().rows.len(), 4);
    assert!(read("hr").await.is_err());
}

#[tokio::test]
async fn cli_refuses_a_live_directory_and_round_trips_a_stopped_one() {
    let temp = TempDir::new().unwrap();
    let (data, backup_dir, restored) = (
        temp.path().join("data"),
        temp.path().join("backup"),
        temp.path().join("restored"),
    );
    populate(&data).await;
    let cli = |args: &[&Path]| {
        let out = Command::new(env!("CARGO_BIN_EXE_inputlayer-backup"))
            .args(args)
            .current_dir(temp.path())
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&out.stdout).into_owned()
            + &String::from_utf8_lossy(&out.stderr);
        (out.status.success(), text)
    };
    let create: [&Path; 4] = ["create".as_ref(), &backup_dir, "--data-dir".as_ref(), &data];

    // A running server holds this lock; the copy must refuse and write nothing.
    let live = DataDirLock::acquire(&data).unwrap();
    let (ok, text) = cli(&create);
    assert!(!ok, "{text}");
    assert!(
        text.contains("in use by a running InputLayer server"),
        "{text}"
    );
    assert!(!backup_dir.exists());
    drop(live);

    let (ok, text) = cli(&create);
    assert!(ok, "{text}");
    assert!(text.contains("backup created"), "{text}");

    let (ok, text) = cli(&["verify".as_ref(), &backup_dir]);
    assert!(ok, "{text}");

    let (ok, text) = cli(&["restore".as_ref(), &backup_dir, &restored]);
    assert!(ok, "{text}");
    assert!(text.contains("validated: the engine loads"), "{text}");
    assert!(
        text.contains("graph: 2 relations, 10 facts, 2 rules"),
        "{text}"
    );

    // Running the same restore again must not touch the restored directory.
    let (ok, text) = cli(&["restore".as_ref(), &backup_dir, &restored]);
    assert!(!ok);
    assert!(text.contains("already exists"), "{text}");
}

/// The message rows of running `program` on `kg` as `principal`.
async fn run(handler: &Handler, kg: &str, program: &str, principal: &Principal) -> Vec<String> {
    let result = handler
        .execute_program(None, Some(kg.into()), program.into(), Some(principal))
        .await
        .unwrap();
    assert!(result.errors.is_empty(), "{program}: {:?}", result.errors);
    result
        .rows
        .iter()
        .map(|r| serde_json::to_string(&r.values[0]).unwrap())
        .collect()
}

#[tokio::test]
async fn online_export_of_a_running_server_restores_what_it_served() {
    let temp = TempDir::new().unwrap();
    let (data, restored) = (temp.path().join("data"), temp.path().join("restored"));
    populate(&data).await;
    let handler = open(&data);
    let expected = observe(&handler).await;
    let admin = admin(&handler);

    // Only admins may write backups on the server's filesystem.
    let mallory = handler.authenticate_user("mallory", "password-m").unwrap();
    for command in [".backup", ".backup status"] {
        let denied = handler
            .execute_program(None, Some("graph".into()), command.into(), Some(&mallory))
            .await;
        assert!(denied.is_err(), "{command} allowed for a viewer");
    }

    let started = run(&handler, "graph", ".backup nightly", &admin).await;
    assert!(
        started[0].contains("Exporting the checkpoint at revision"),
        "{started:?}"
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let status = loop {
        let status = run(&handler, "graph", ".backup status", &admin).await;
        if !status[0].contains("Exporting") {
            break status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "export never finished"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    assert!(status[0].contains("Last export complete"), "{status:?}");

    let export = temp.path().join("backups/nightly");
    let manifest = Manifest::load(&export).unwrap();
    assert!(manifest.revision.is_some());
    assert!(
        started[0].contains(&format!("revision {}", manifest.revision.unwrap())),
        "{started:?}"
    );
    backup::verify(&export).unwrap();
    backup::restore(&export, &restored).unwrap();
    assert_eq!(observe(&open(&restored)).await, expected);

    // A second export may not reuse a name.
    let again = handler
        .execute_program(
            None,
            Some("graph".into()),
            ".backup nightly".into(),
            Some(&admin),
        )
        .await
        .unwrap_err();
    assert!(again.message.contains("already exists"), "{again}");
}
