//! A credential revoked while a program runs gets none of its output, on
//! every return path. `.why` returns from its own path, after searching its
//! proof outside the storage guard; the revocation lands deterministically
//! between admission and that return, through the meta-dispatch seam.

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

const KG: &str = "shared";
const KEY_LABEL: &str = "bob-why";
const PROOF: &str = ".why full ?path(X, Y)";

/// A handler with auth bootstrapped, KG `shared` holding a two-row proof
/// fixture, and `bob` (viewer on `shared`) owning API key `bob-why`.
async fn handler_with_proof_fixture() -> (Arc<Handler>, String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    config.http.auth.bootstrap_admin_password = Some("admin-pw".to_string());
    config.http.auth.credentials_file = Some(tmp.path().join("credentials.toml"));
    let handler = Arc::new(Handler::from_config(config).expect("handler creation failed"));
    handler.bootstrap_auth();
    handler
        .storage
        .read()
        .create_knowledge_graph(KG)
        .expect("knowledge graph creation failed");
    handler
        .handle_user_create("bob", "bob-pw", "editor")
        .expect("user creation failed");
    handler
        .handle_kg_acl_grant(KG, "bob", "viewer")
        .expect("acl grant failed");
    handler
        .execute_program(
            None,
            Some(KG.to_string()),
            "+edge[(1, 2), (2, 3)]\n+path(X, Y) <- edge(X, Y)".to_string(),
            None,
        )
        .await
        .expect("fixture setup failed");
    let key = handler
        .create_api_key(KEY_LABEL, "bob", None)
        .expect("key creation failed");
    (handler, key, tmp)
}

async fn run_proof(
    handler: &Handler,
    principal: &crate::auth::Principal,
) -> Result<QueryResult, String> {
    handler
        .execute_program(
            None,
            Some(KG.to_string()),
            PROOF.to_string(),
            Some(principal),
        )
        .await
}

// Current-thread runtime: the hook is set on the thread that polls
// `execute_program`, which carries it to the job's blocking thread.
#[tokio::test]
async fn revocation_during_a_proof_withholds_it() {
    let (handler, key, _tmp) = handler_with_proof_fixture().await;
    let principal = handler
        .authenticate_api_key(&key)
        .expect("key must authenticate");

    let delivered = run_proof(&handler, &principal)
        .await
        .expect("an unrevoked principal gets its proof");
    assert_eq!(delivered.proof_trees.map(|trees| trees.len()), Some(2));

    let revoked = Arc::new(AtomicBool::new(false));
    {
        let handler = Arc::clone(&handler);
        let revoked = Arc::clone(&revoked);
        meta_dispatch_hook::set(move || {
            handler
                .handle_apikey_revoke(KEY_LABEL)
                .expect("revocation failed");
            revoked.store(true, Ordering::SeqCst);
        });
    }
    let withheld = run_proof(&handler, &principal).await;
    assert!(
        revoked.load(Ordering::SeqCst),
        "the revocation must land while the proof runs"
    );
    match withheld {
        Err(e) => assert_eq!(e, String::from(crate::auth::CredentialEnded::Revoked)),
        Ok(result) => panic!(
            "revoked mid-proof, yet {:?} proof trees were delivered",
            result.proof_trees.map(|trees| trees.len())
        ),
    }
}
