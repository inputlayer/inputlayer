//! Extract-only incremental turns (`POST /v1/conversations/{id}/turns`).
//!
//! A turn delivers ONLY the new messages. The KG ledger supplies the rest:
//! the global index the batch starts at, recent context, and the live
//! prior rows the extractor may retract. Only the new messages are
//! extracted (one model call, prompt-cached head), their claims are quote
//! checked against the global index they cite, and everything lands in one
//! write batch. No completion is produced.

use crate::engine_pool::EnginePool;
use crate::ledger::{render_context, render_digest};
use crate::model::{render_messages, Extractor};
use crate::ontology::{LoadedOntology, PromptSlots};
use crate::pipeline::{evaluate, read_prior, EvalOutcome, EvalRequest, Mode};
use anyhow::Result;
use serde_json::json;

pub struct TurnOutcome {
    /// Global index of the first delivered message.
    pub first_index: usize,
    pub count: usize,
    pub eval: EvalOutcome,
}

/// Run one turn for one (kg, ontology) pair. The caller holds the
/// conversation's lock for the duration.
#[allow(clippy::too_many_arguments)]
pub async fn run_turn(
    pool: &EnginePool,
    extractor: &dyn Extractor,
    ontology: &LoadedOntology,
    kg: &str,
    conversation: &str,
    messages: &[(String, String)],
    current_date: &str,
    want_trace: bool,
) -> Result<TurnOutcome> {
    let prior = read_prior(pool, ontology, kg, conversation).await?;
    let first_index = prior.next_index;

    let quote_fields: Vec<&str> = ontology
        .manifest
        .validate
        .quote
        .as_ref()
        .map(|q| vec![q.field.as_str(), q.within.as_str()])
        .unwrap_or_default();
    let digest = render_digest(&prior.rows, &ontology.schema, conversation, &quote_fields);
    let context = render_context(&prior.context);
    let new_messages = render_messages(first_index, messages);
    let prompt = ontology.render_prompt(&PromptSlots {
        current_date,
        claims_digest: &digest,
        prior_messages: &context,
        new_messages: &new_messages,
    });

    let started = std::time::Instant::now();
    let extraction = extractor
        .extract(
            &ontology.extraction_model,
            &prompt.system,
            &prompt.user,
            &ontology.schema,
        )
        .await?;
    let extract_ms = started.elapsed().as_millis();

    let request = EvalRequest {
        kg,
        prefix: conversation,
        messages,
        first_index,
        mode: Mode::Turn { prior: &prior },
        want_trace,
    };
    let mut eval = evaluate(pool, ontology, &request, extraction.output).await?;
    if let Some(trace) = eval.trace.as_mut().and_then(|t| t.as_object_mut()) {
        trace.insert("model".to_string(), json!(ontology.extraction_model));
        trace.insert("extract_ms".to_string(), json!(extract_ms));
        trace.insert("usage".to_string(), extraction.usage);
        trace.insert("prompt".to_string(), json!(prompt.user));
    }
    Ok(TurnOutcome {
        first_index,
        count: messages.len(),
        eval,
    })
}
