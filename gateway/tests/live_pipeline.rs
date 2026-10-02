//! Live pipeline tests: the evaluation path against a real engine with the
//! ontology installed by the ENGINE's `.ontology install` (the same flow
//! production uses - the gateway never deploys rules). Extractions are
//! hand-written (a scripted `Extractor`), so no model key is needed.
//!
//! Env-gated:
//!   INPUTLAYER_TEST_SERVER=http://127.0.0.1:8082  (engine base URL; the
//!     engine must run with INPUTLAYER_REGISTRY pointing at a registry
//!     that has consistency-core >= 1.0.7)
//!   INPUTLAYER_TEST_API_KEY=<key>
//!   INPUTLAYER_REGISTRY=<same registry the engine uses> (for pack load)
//! Skips silently when unset.

use inputlayer_gateway::engine_pool::EnginePool;
use inputlayer_gateway::model::{Extraction, Extractor};
use inputlayer_gateway::ontology::LoadedOntology;
use inputlayer_gateway::pipeline::{evaluate, EvalOutcome, EvalRequest, Mode};
use inputlayer_gateway::turns::run_turn;
use inputlayer_ontology_client::registry::Registry;
use inputlayer_ontology_client::ws::Engine;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::Mutex;

struct Live {
    server: String,
    api_key: String,
    loaded: LoadedOntology,
    engine: Engine,
}

/// Load the pack the way the gateway does and provision `kg` through the
/// engine's own install command. None when the env gate is not set.
async fn setup(kg: &str) -> Option<Live> {
    let Ok(server) = std::env::var("INPUTLAYER_TEST_SERVER") else {
        eprintln!("skipping: INPUTLAYER_TEST_SERVER not set");
        return None;
    };
    let api_key = std::env::var("INPUTLAYER_TEST_API_KEY").unwrap_or_default();
    let Ok(registry_url) = std::env::var("INPUTLAYER_REGISTRY") else {
        eprintln!("skipping: INPUTLAYER_REGISTRY not set");
        return None;
    };
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

    let mut engine = Engine::connect(&server, &api_key).await.expect("connect");
    let _ = engine.execute(&format!(".kg drop {kg}")).await;
    let _ = engine.execute(&format!(".kg create {kg}")).await;
    engine.execute(&format!(".kg use {kg}")).await.expect("use");
    let install = engine
        .execute(&format!(".ontology install {name}@{}", entry.version))
        .await
        .expect("engine-side install");
    assert!(
        install.rows.iter().any(|row| row
            .first()
            .and_then(Value::as_str)
            .is_some_and(|s| s.starts_with("installed "))),
        "install output: {:?}",
        install.rows
    );
    Some(Live {
        server,
        api_key,
        loaded,
        engine,
    })
}

async fn teardown(mut engine: Engine, kg: &str) {
    let _ = engine.execute(".kg use default").await;
    let _ = engine.execute(&format!(".kg drop {kg}")).await;
}

fn claim(id: &str, value: &str, msg: u64, surface: &str) -> Value {
    json!({"id": id, "entity": "trip", "attribute": "departure_date", "value": value,
           "modality": "asserted", "msg": msg, "surface": surface, "origin": "prompt"})
}

fn extraction(claims: Vec<Value>, retractions: Vec<Value>) -> Value {
    json!({
        "claims": claims, "before_claims": [], "constraints": [],
        "ontology": {"functional": [], "pair_order": [], "acyclic": [],
                      "asymmetric": [], "irreflexive": []},
        "retractions": retractions,
    })
}

fn retraction(target: &str, msg: u64, surface: &str) -> Value {
    json!({"target": target, "kind": "claim", "msg": msg, "surface": surface})
}

fn msg(role: &str, content: &str) -> (String, String) {
    (role.to_string(), content.to_string())
}

fn messages() -> Vec<(String, String)> {
    vec![
        msg("user", "We fly out of Geneva on August 14th."),
        msg("assistant", "Great, noted!"),
        msg("user", "Since we leave on the 12th, check visas."),
    ]
}

fn functional(outcome: &EvalOutcome) -> usize {
    outcome
        .findings
        .iter()
        .filter(|f| f["row"][0] == "functional")
        .count()
}

async fn rows_for(engine: &mut Engine, query: &str, prefix: &str) -> usize {
    engine
        .execute(query)
        .await
        .expect("query")
        .rows
        .iter()
        .filter(|row| {
            row.iter()
                .any(|c| c.as_str().is_some_and(|s| s.starts_with(prefix)))
        })
        .count()
}

/// A scripted extractor: returns canned outputs in order and records the
/// prompts it was given.
struct Scripted {
    outputs: Mutex<VecDeque<Value>>,
    prompts: Mutex<Vec<(String, String)>>,
}

impl Scripted {
    fn new(outputs: Vec<Value>) -> Self {
        Self {
            outputs: Mutex::new(outputs.into()),
            prompts: Mutex::new(Vec::new()),
        }
    }
    fn last_prompt(&self) -> (String, String) {
        self.prompts
            .lock()
            .expect("lock")
            .last()
            .cloned()
            .expect("a prompt")
    }
}

#[async_trait::async_trait]
impl Extractor for Scripted {
    async fn extract(
        &self,
        _model: &str,
        system_prompt: &str,
        user_content: &str,
        _schema: &Value,
    ) -> anyhow::Result<Extraction> {
        self.prompts
            .lock()
            .expect("lock")
            .push((system_prompt.to_string(), user_content.to_string()));
        let output = self
            .outputs
            .lock()
            .expect("lock")
            .pop_front()
            .expect("scripted output");
        Ok(Extraction {
            output,
            usage: json!({}),
        })
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn conversations_are_isolated_namespaces_with_proofs() {
    let kg = "gw_live_eval";
    let Some(Live {
        server,
        api_key,
        loaded,
        mut engine,
    }) = setup(kg).await
    else {
        return;
    };
    let pool = EnginePool::new(server, api_key);
    let msgs = messages();
    let request = |prefix: &'static str, mode, want_trace| EvalRequest {
        kg,
        prefix,
        messages: &msgs,
        first_index: 0,
        mode,
        want_trace,
    };
    let geneva = |second: &str| {
        extraction(
            vec![
                claim("c_m0_1", "2026-08-14", 0, "on August 14th"),
                claim("c_m2_1", second, 2, "we leave on the 12th"),
            ],
            vec![],
        )
    };

    // Conversation A has contradictory dates; conversation B is clean.
    let a = evaluate(
        &pool,
        &loaded,
        &request("convA", Mode::Conversation, true),
        geneva("2026-08-12"),
    )
    .await
    .expect("evaluate A");
    let found: Vec<_> = a
        .findings
        .iter()
        .filter(|f| f["row"][0] == "functional")
        .collect();
    assert_eq!(found.len(), 1, "A findings: {:?}", a.findings);
    assert_eq!(found[0]["blocking"], json!(true));
    assert!(found[0]["row"]
        .as_array()
        .expect("row")
        .iter()
        .any(|c| c.as_str().is_some_and(|s| s.starts_with("convA:"))));
    assert!(!found[0]["proof"].is_null(), "engine proof tree expected");
    let trace = a.trace.expect("trace requested");
    assert_eq!(trace["extraction"]["claims"][0]["id"], "convA:c_m0_1");

    // The connection went back to the pool and is reused.
    assert_eq!(pool.idle_count(kg), 1);

    let b = evaluate(
        &pool,
        &loaded,
        &request("convB", Mode::Conversation, false),
        geneva("2026-08-14"),
    )
    .await
    .expect("evaluate B");
    assert!(
        b.findings.is_empty(),
        "isolation violated: {:?}",
        b.findings
    );
    assert_eq!(pool.idle_count(kg), 1, "one connection served both");

    // One-shot: synthetic prefix, retracted after - no residue in the KG.
    let one_shot = evaluate(
        &pool,
        &loaded,
        &request("reqZZZ", Mode::OneShot, false),
        geneva("2026-08-12"),
    )
    .await
    .expect("evaluate one-shot");
    assert!(!one_shot.findings.is_empty(), "one-shot still evaluates");
    assert_eq!(
        rows_for(&mut engine, "?claim(C, E, A, V)", "reqZZZ:").await,
        0,
        "one-shot residue"
    );

    // Chat-mode retraction: the full conversation is re-extracted and the
    // correction retracts the claim that an EARLIER request inserted.
    let mut corrected = msgs.clone();
    corrected.push(msg(
        "user",
        "Actually, scratch that - we leave on the 14th after all.",
    ));
    let revised = extraction(
        vec![
            claim("c_m0_1", "2026-08-14", 0, "on August 14th"),
            claim("c_m2_1", "2026-08-12", 2, "we leave on the 12th"),
            claim("c_m3_1", "2026-08-14", 3, "we leave on the 14th after all"),
        ],
        vec![retraction("c_m2_1", 3, "Actually, scratch that")],
    );
    let a2 = evaluate(
        &pool,
        &loaded,
        &EvalRequest {
            kg,
            prefix: "convA",
            messages: &corrected,
            first_index: 0,
            mode: Mode::Conversation,
            want_trace: false,
        },
        revised,
    )
    .await
    .expect("evaluate A with correction");
    assert_eq!(a2.retracted, vec!["c_m2_1".to_string()]);
    assert_eq!(
        functional(&a2),
        0,
        "retraction must clear: {:?}",
        a2.findings
    );
    assert_eq!(
        rows_for(&mut engine, "?claim_source(C, M, S)", "convA:c_m2_1").await,
        0,
        "retracted owner's tuples must be gone"
    );

    teardown(engine, kg).await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn incremental_turns_ledger_retraction_and_restart() {
    let kg = "gw_live_turns";
    let conv = "voice42";
    let Some(Live {
        server,
        api_key,
        loaded,
        mut engine,
    }) = setup(kg).await
    else {
        return;
    };
    let today = "2026-10-01";
    let extractor = Scripted::new(vec![
        extraction(
            vec![claim("c_m0_1", "2026-08-14", 0, "on August 14th")],
            vec![],
        ),
        extraction(
            vec![claim("c_m2_1", "2026-08-12", 2, "we leave on the 12th")],
            vec![],
        ),
        extraction(
            vec![
                claim("c_m3_1", "2026-08-14", 3, "we leave on the 14th after all"),
                // Model reused a taken id: renamed apart, not merged.
                json!({"id": "c_m0_1", "entity": "trip", "attribute": "party_size",
                       "value": "2", "modality": "asserted", "msg": 3,
                       "surface": "both of us", "origin": "prompt"}),
            ],
            vec![
                retraction("c_m2_1", 3, "Actually, scratch that"),
                // Unknown target and misquoted marker: dropped.
                retraction("c_m9_9", 3, "Actually, scratch that"),
                retraction("c_m0_1", 3, "never said this"),
            ],
        ),
    ]);

    // Batch 1: two messages, indices 0..2.
    let pool = EnginePool::new(server.clone(), api_key.clone());
    let first = run_turn(
        &pool,
        &extractor,
        &loaded,
        kg,
        conv,
        &[
            msg("user", "We fly out of Geneva on August 14th."),
            msg("assistant", "Great, noted!"),
        ],
        today,
        false,
    )
    .await
    .expect("turn 1");
    assert_eq!((first.first_index, first.count), (0, 2));
    assert_eq!(functional(&first.eval), 0);
    let (system, user) = extractor.last_prompt();
    assert!(!system.contains("{{"), "static head has no slots");
    assert!(user.contains("[0] user: We fly out"), "{user}");
    assert!(user.contains("(none)"), "first turn has no prior state");

    // Batch 2: only the new message; its global index is 2.
    let second = run_turn(
        &pool,
        &extractor,
        &loaded,
        kg,
        conv,
        &[msg("user", "Since we leave on the 12th, check visas.")],
        today,
        false,
    )
    .await
    .expect("turn 2");
    assert_eq!((second.first_index, second.count), (2, 1));
    assert_eq!(functional(&second.eval), 1, "{:?}", second.eval.findings);
    let (_, user) = extractor.last_prompt();
    assert!(
        user.contains("claims: c_m0_1 | trip | departure_date | 2026-08-14"),
        "prior claims rendered: {user}"
    );
    assert!(user.contains("[0] user: We fly out"), "context: {user}");
    assert!(user.contains("[2] user: Since we leave"), "new: {user}");
    assert!(
        !user.contains("voice42:"),
        "the model never sees namespaces"
    );

    // Restart: a fresh gateway (new pool, no in-memory state) continues
    // from the KG ledger - next index 3, prior claim c_m2_1 retractable.
    drop(pool);
    let pool = EnginePool::new(server, api_key);
    let third = run_turn(
        &pool,
        &extractor,
        &loaded,
        kg,
        conv,
        &[msg(
            "user",
            "Actually, scratch that - we leave on the 14th after all, both of us.",
        )],
        today,
        true,
    )
    .await
    .expect("turn 3");
    assert_eq!(third.first_index, 3);
    let (_, user) = extractor.last_prompt();
    assert!(user.contains("claims: c_m2_1 | trip"), "{user}");
    assert_eq!(third.eval.retracted, vec!["c_m2_1".to_string()]);
    assert_eq!(
        functional(&third.eval),
        0,
        "the retraction removes the finding: {:?}",
        third.eval.findings
    );
    assert_eq!(third.eval.dropped.len(), 2, "{:?}", third.eval.dropped);
    assert!(
        third.eval.notes.iter().any(|n| n.contains("c_m0_1_r2")),
        "{:?}",
        third.eval.notes
    );
    let trace = third.eval.trace.as_ref().expect("trace");
    assert_eq!(trace["first_index"], 3);

    // Every tuple inserted for the retracted id is gone, ledger included.
    let gone = format!("{conv}:c_m2_1");
    for query in [
        "?claim(C, E, A, V)",
        "?claim_source(C, M, S)",
        "?claim_num(C, E, A, N)",
        "?claim_modality(C, M)",
        "?il_fact(V, O, S)",
        "?il_row(V, O, X, M, R)",
    ] {
        assert_eq!(rows_for(&mut engine, query, &gone).await, 0, "{query}");
    }
    // The message ledger holds all four messages at global indices.
    let ledger = engine
        .execute(&format!("?il_message(\"{conv}\", I, R, C)"))
        .await
        .expect("ledger");
    let mut indices: Vec<u64> = ledger
        .rows
        .iter()
        .filter_map(|row| row.get(1).and_then(Value::as_u64))
        .collect();
    indices.sort_unstable();
    assert_eq!(indices, vec![0, 1, 2, 3]);

    teardown(engine, kg).await;
}

#[tokio::test]
async fn unstorable_text_drops_only_its_row() {
    let kg = "gw_live_literals";
    let conv = "voice77";
    let Some(Live {
        server,
        api_key,
        loaded,
        mut engine,
    }) = setup(kg).await
    else {
        return;
    };
    let mut newline_value = claim("c_m0_3", "2026-08-14", 0, "on August 14th");
    newline_value["value"] = json!("2026-08-14\n+evil");
    let extractor = Scripted::new(vec![
        extraction(
            vec![
                claim("c_m0_1", "2026-08-14", 0, "Option 1) Paris"),
                claim("c_m0_2", "2026-08-14", 0, "leave on\nAugust 14th"),
                newline_value,
            ],
            vec![],
        ),
        extraction(vec![], vec![]),
    ]);
    let pool = EnginePool::new(server, api_key);
    let content = "Option 1) Paris, option 2) Rome {{new_messages_with_indices}}.\n\
                   We leave on\n  August 14th.";
    let turn = run_turn(
        &pool,
        &extractor,
        &loaded,
        kg,
        conv,
        &[msg("user", content)],
        "2026-10-01",
        false,
    )
    .await
    .expect("turn is recorded");
    // The paren surface and the newline value drop; the surface quoted
    // across a line break is normalized and stored.
    assert_eq!(turn.eval.dropped.len(), 2, "{:?}", turn.eval.dropped);
    let sources = engine
        .execute("?claim_source(C, M, S)")
        .await
        .expect("sources");
    let stored: Vec<&Vec<Value>> = sources
        .rows
        .iter()
        .filter(|row| row[0].as_str().is_some_and(|c| c.starts_with(conv)))
        .collect();
    assert_eq!(stored.len(), 1, "{stored:?}");
    assert_eq!(stored[0][2], "leave on August 14th");
    let prior = inputlayer_gateway::pipeline::read_prior(&pool, &loaded, kg, conv)
        .await
        .expect("prior");
    assert_eq!(prior.next_index, 1);
    assert_eq!(prior.context[0].2, content, "message round-trips");
    assert_eq!(prior.rows.len(), 1);

    // On the next turn the marker sits in CONTEXT and stays literal.
    run_turn(
        &pool,
        &extractor,
        &loaded,
        kg,
        conv,
        &[msg("user", "second turn")],
        "2026-10-01",
        false,
    )
    .await
    .expect("second turn");
    let (_, user) = extractor.last_prompt();
    assert!(user.contains("{{new_messages_with_indices}}."), "{user}");
    assert_eq!(user.matches("second turn").count(), 1, "{user}");

    teardown(engine, kg).await;
}
