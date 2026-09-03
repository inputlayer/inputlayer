//! Live pipeline test: the evaluation path against a real engine with the
//! ontology installed by the ENGINE's `.ontology install` (the same flow
//! production uses - the gateway never deploys rules).
//!
//! Env-gated:
//!   INPUTLAYER_TEST_SERVER=http://127.0.0.1:8082  (engine base URL; the
//!     engine must run with INPUTLAYER_REGISTRY pointing at a registry
//!     that has consistency-core >= 1.0.3)
//!   INPUTLAYER_TEST_API_KEY=<key>
//!   INPUTLAYER_REGISTRY=<same registry the engine uses> (for pack load)
//! Skips silently when unset.

use inputlayer_gateway::ontology::LoadedOntology;
use inputlayer_gateway::pipeline::{evaluate, EngineConfig};
use inputlayer_ontology_client::registry::Registry;
use inputlayer_ontology_client::ws::Engine;
use serde_json::json;

fn geneva_extraction(first: &str, second: &str) -> serde_json::Value {
    json!({
        "claims": [
            {"id": "c_m0_1", "entity": "trip", "attribute": "departure_date",
             "value": first, "modality": "asserted", "msg": 0,
             "surface": "on August 14th", "origin": "prompt"},
            {"id": "c_m2_1", "entity": "trip", "attribute": "departure_date",
             "value": second, "modality": "asserted", "msg": 2,
             "surface": "we leave on the 12th", "origin": "prompt"}
        ],
        "before_claims": [], "constraints": [],
        "ontology": {"functional": [], "pair_order": [], "acyclic": [],
                      "asymmetric": [], "irreflexive": []}
    })
}

fn messages() -> Vec<(String, String)> {
    vec![
        (
            "user".to_string(),
            "We fly out of Geneva on August 14th.".to_string(),
        ),
        ("assistant".to_string(), "Great, noted!".to_string()),
        (
            "user".to_string(),
            "Since we leave on the 12th, check visas.".to_string(),
        ),
    ]
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn conversations_are_isolated_namespaces_with_proofs() {
    let Ok(server) = std::env::var("INPUTLAYER_TEST_SERVER") else {
        eprintln!("skipping: INPUTLAYER_TEST_SERVER not set");
        return;
    };
    let api_key = std::env::var("INPUTLAYER_TEST_API_KEY").unwrap_or_default();
    let Ok(registry_url) = std::env::var("INPUTLAYER_REGISTRY") else {
        eprintln!("skipping: INPUTLAYER_REGISTRY not set");
        return;
    };

    // Load the pack the way the gateway does at startup.
    let registry = Registry::new(registry_url, None);
    let (name, entry) = registry
        .resolve("consistency-core")
        .await
        .expect("resolve pack");
    let entry_dir = registry.fetch(&name, &entry).await.expect("fetch pack");
    let loaded = LoadedOntology::load(&entry_dir, &entry.digest).expect("load pack");
    assert!(
        loaded
            .manifest
            .report
            .watch
            .iter()
            .any(|w| w.blocking && w.proof.is_some()),
        "pack must declare blocking watch views with proof templates (1.0.3+)"
    );
    assert!(
        !loaded.manifest.extraction.identifier_fields.is_empty(),
        "pack must declare identifier_fields (1.0.3+)"
    );

    // Provision the KG through the ENGINE's own install command.
    let mut engine = Engine::connect(&server, &api_key).await.expect("connect");
    let kg = "gw_live_eval";
    let _ = engine.execute(&format!(".kg create {kg}")).await;
    engine.execute(&format!(".kg use {kg}")).await.expect("use");
    let install = engine
        .execute(&format!(".ontology install {name}@{}", entry.version))
        .await
        .expect("engine-side install");
    assert!(
        install.rows.iter().any(|row| row
            .first()
            .and_then(serde_json::Value::as_str)
            .is_some_and(|s| s.starts_with("installed "))),
        "install output: {:?}",
        install.rows
    );

    let engine_config = EngineConfig {
        url: server,
        api_key,
    };

    // Conversation A has contradictory dates; conversation B is clean.
    let a = evaluate(
        &engine_config,
        &loaded,
        kg,
        "convA",
        geneva_extraction("2026-08-14", "2026-08-12"),
        &messages(),
        true,
        false,
    )
    .await
    .expect("evaluate A");
    let functional: Vec<_> = a
        .findings
        .iter()
        .filter(|f| f["row"][0] == "functional")
        .collect();
    assert_eq!(functional.len(), 1, "A findings: {:?}", a.findings);
    assert_eq!(functional[0]["blocking"], json!(true));
    // Prefixed identifiers appear in the finding row; proof came from the
    // engine's .why (non-null for the conflict).
    assert!(functional[0]["row"]
        .as_array()
        .expect("row")
        .iter()
        .any(|c| c.as_str().is_some_and(|s| s.starts_with("convA:"))));
    assert!(
        !functional[0]["proof"].is_null(),
        "engine proof tree expected"
    );
    // Trace carries the prefixed extraction.
    let trace = a.trace.expect("trace requested");
    assert_eq!(trace["extraction"]["claims"][0]["id"], "convA:c_m0_1");

    // Conversation B shares the KG but must see NO findings of its own,
    // and A's findings must not leak into B's view.
    let b = evaluate(
        &engine_config,
        &loaded,
        kg,
        "convB",
        geneva_extraction("2026-08-14", "2026-08-14"),
        &messages(),
        false,
        false,
    )
    .await
    .expect("evaluate B");
    assert!(
        b.findings.is_empty(),
        "conversation isolation violated: {:?}",
        b.findings
    );

    // One-shot: synthetic prefix, retracted after - no residue in the KG.
    let one_shot = evaluate(
        &engine_config,
        &loaded,
        kg,
        "reqZZZ",
        geneva_extraction("2026-08-14", "2026-08-12"),
        &messages(),
        false,
        true,
    )
    .await
    .expect("evaluate one-shot");
    assert!(!one_shot.findings.is_empty(), "one-shot still evaluates");
    let leftovers = engine
        .execute("?claim(C, E, A, V)")
        .await
        .expect("query claims");
    assert!(
        !leftovers.rows.iter().any(|row| row
            .first()
            .and_then(serde_json::Value::as_str)
            .is_some_and(|s| s.starts_with("reqZZZ:"))),
        "one-shot residue found: {:?}",
        leftovers.rows
    );

    // Cleanup.
    let _ = engine.execute(".kg use default").await;
    let _ = engine.execute(&format!(".kg drop {kg}")).await;
}
