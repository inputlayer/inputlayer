//! Live pipeline test: everything below the model call, against a real
//! engine and the real (cached, digest-verified) registry entry.
//!
//! Env-gated like the SDK's integration tests:
//!   INPUTLAYER_TEST_SERVER=http://127.0.0.1:8082  (engine base URL)
//!   INPUTLAYER_TEST_API_KEY=<key>
//! Skips silently when unset. Requires consistency-core in the local
//! ontology cache (an `il fetch`/`il install` leaves it there).

use inputlayer_gateway::ontology::LoadedOntology;
use inputlayer_gateway::pipeline::{run_verify, EngineConfig};
use serde_json::json;

fn cached_entry() -> Option<std::path::PathBuf> {
    let dir = inputlayer_ontology_client::registry::cache_dir().ok()?;
    let base = dir.join("consistency-core");
    let version = std::fs::read_dir(&base)
        .ok()?
        .filter_map(Result::ok)
        .find(|e| {
            e.path()
                .join("consistency-core")
                .join("ontology.toml")
                .is_file()
        })?;
    Some(version.path().join("consistency-core"))
}

#[tokio::test]
async fn verify_pipeline_finds_functional_conflict_live() {
    let Ok(server) = std::env::var("INPUTLAYER_TEST_SERVER") else {
        eprintln!("skipping: INPUTLAYER_TEST_SERVER not set");
        return;
    };
    let api_key = std::env::var("INPUTLAYER_TEST_API_KEY").unwrap_or_default();
    let Some(entry_dir) = cached_entry() else {
        eprintln!("skipping: consistency-core not in ontology cache");
        return;
    };

    let loaded = LoadedOntology::load(&entry_dir, "sha256:test").expect("load entry");
    assert!(
        loaded.prompt_template.contains("{{current_date}}") || !loaded.prompt_template.is_empty(),
        "prompt extracted from fenced block"
    );
    assert_eq!(
        loaded.schema["additionalProperties"],
        json!(false),
        "schema normalized for structured outputs"
    );

    let messages = vec![
        (
            "user".to_string(),
            "We fly out of Geneva on August 14th.".to_string(),
        ),
        ("assistant".to_string(), "Great, noted!".to_string()),
        (
            "user".to_string(),
            "Since we leave on the 12th, check visas.".to_string(),
        ),
    ];
    // Canned extraction standing in for the model: two conflicting departure
    // dates with verbatim quotes, plus one claim whose quote is NOT verbatim
    // (must be dropped, never inserted).
    let extraction = json!({
        "claims": [
            {"id": "c1", "entity": "trip", "attribute": "departure_date",
             "value": "2026-08-14", "modality": "asserted", "msg": 0,
             "surface": "on August 14th", "origin": "prompt"},
            {"id": "c2", "entity": "trip", "attribute": "departure_date",
             "value": "2026-08-12", "modality": "asserted", "msg": 2,
             "surface": "we leave on the 12th", "origin": "prompt"},
            {"id": "c3", "entity": "trip", "attribute": "departure_city",
             "value": "lyon", "modality": "asserted", "msg": 0,
             "surface": "definitely Lyon", "origin": "prompt"}
        ],
        "before_claims": [], "constraints": [],
        "ontology": {"functional": [], "pair_order": [], "acyclic": [],
                      "asymmetric": [], "irreflexive": []}
    });

    let engine = EngineConfig {
        url: server,
        api_key,
    };
    let outcome = run_verify(&engine, &loaded, extraction, &messages, true)
        .await
        .expect("pipeline");

    // Trace carries the validated extraction (the dropped claim is gone),
    // the mapped statements, and the ephemeral KG name.
    let trace = outcome.trace.as_ref().expect("trace requested");
    assert_eq!(
        trace["extraction"]["claims"].as_array().map(Vec::len),
        Some(2)
    );
    assert!(trace["statements"]
        .as_array()
        .is_some_and(|s| !s.is_empty()));
    assert!(trace["kg"]
        .as_str()
        .is_some_and(|k| k.starts_with("_verify_")));
    assert!(trace["engine_ms"].is_number());

    assert_eq!(outcome.status, "conflicts_found");
    assert_eq!(
        outcome.dropped.len(),
        1,
        "non-verbatim quote dropped: {:?}",
        outcome.dropped
    );
    assert!(outcome.dropped[0].contains("definitely Lyon"));

    // Exactly one functional finding after symmetric dedup, carrying both
    // quoted spans.
    let functional: Vec<_> = outcome
        .findings
        .iter()
        .filter(|f| f["row"][0] == "functional")
        .collect();
    assert_eq!(functional.len(), 1, "findings: {:?}", outcome.findings);
    let spans = functional[0]["spans"].as_array().expect("spans");
    let surfaces: Vec<&str> = spans.iter().filter_map(|s| s["surface"].as_str()).collect();
    assert!(surfaces.contains(&"on August 14th"));
    assert!(surfaces.contains(&"we leave on the 12th"));
}
