//! A fact write whose WAL file was removed or moved while the server ran is never
//! acknowledged (issue #93): the client gets `outcome_unknown`, every later write
//! `store_read_only`, and the write is not visible in the live store.

use inputlayer::config::DurabilityMode;
use inputlayer::protocol::{ErrorCode, Handler, ProgramError, QueryResult};
use inputlayer::{Config, StorageEngine};
use std::fs;
use std::path::Path;
use tempfile::TempDir;

const KG: &str = "default";

fn handler(dir: &Path, durability_mode: DurabilityMode) -> Handler {
    crate::harness::pool();
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    config.storage.persist.durability_mode = durability_mode;
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

async fn claims(handler: &Handler) -> usize {
    run(handler, "?claim(C, V)")
        .await
        .expect("query")
        .rows
        .len()
}

fn code(result: Result<QueryResult, ProgramError>) -> Option<ErrorCode> {
    match result {
        Ok(result) => panic!("write acknowledged: {:?}", result.rows),
        Err(error) => error.code,
    }
}

#[tokio::test]
async fn inserts_after_the_data_directory_is_removed_are_not_acknowledged() {
    for mode in [DurabilityMode::Immediate, DurabilityMode::Batched] {
        let temp = TempDir::new().expect("tempdir");
        let data = temp.path().join("data");
        let handler = handler(&data, mode);
        run(&handler, r#"+claim[("c1", 1)]"#).await.expect("insert");
        fs::remove_dir_all(&data).expect("remove data dir");

        let first = run(&handler, r#"+claim[("c2", 2)]"#).await;
        assert_eq!(code(first), Some(ErrorCode::OutcomeUnknown), "{mode:?}");
        assert_eq!(
            claims(&handler).await,
            1,
            "{mode:?}: refused write is visible"
        );

        let next = run(&handler, r#"+claim[("c3", 3)]"#).await;
        assert_eq!(code(next), Some(ErrorCode::StoreReadOnly), "{mode:?}");
        let rule = run(&handler, "+big(C) <- claim(C, V), V > 1").await;
        assert_eq!(code(rule), Some(ErrorCode::StoreReadOnly), "{mode:?}");
        assert_eq!(claims(&handler).await, 1, "{mode:?}");
    }
}

#[tokio::test]
async fn write_into_a_moved_data_directory_is_recovered_when_it_is_put_back() {
    let temp = TempDir::new().expect("tempdir");
    let data = temp.path().join("data");
    let moved = temp.path().join("moved");
    {
        let handler = handler(&data, DurabilityMode::Immediate);
        run(&handler, r#"+claim[("c1", 1)]"#).await.expect("insert");
        fs::rename(&data, &moved).expect("move data dir");
        let write = run(&handler, r#"+claim[("c2", 2)]"#).await;
        assert_eq!(code(write), Some(ErrorCode::OutcomeUnknown));
    }
    // Unknown, not failed: the record reached the moved WAL file.
    fs::rename(&moved, &data).expect("move data dir back");
    let handler = handler(&data, DurabilityMode::Immediate);
    assert_eq!(claims(&handler).await, 2);
    run(&handler, r#"+claim[("c3", 3)]"#)
        .await
        .expect("writes resume after restart");
}
