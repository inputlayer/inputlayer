# RFC: Verified Completions

**An OpenAI-compatible chat endpoint with Datalog-backed consistency verification**

| | |
|---|---|
| Status | Draft |
| Target | `inputlayer/inputlayer` |
| Depends on | Core engine (WS API, named KGs, `.why` proof trees), Anthropic API key supplied by operator |
| New surface | `POST /v1/chat/completions`, `POST /v1/verify` |

---

## 1. Summary

InputLayer gains an LLM gateway: an OpenAI-compatible chat-completions endpoint. The operator drops an Anthropic API key into config; clients point their existing OpenAI SDK at InputLayer by changing `base_url`. On every request the gateway (a) forwards the conversation to Claude for the completion and (b) **in parallel**, translates the conversation into facts inside a per-conversation knowledge graph and evaluates a shipped consistency rule pack over them. Contradictions come back attached to the normal completion response as an `inputlayer` extension block — each one with the exact source spans and the `.why` proof tree that derived it.

The engine's three differentiators are load-bearing here, not decorative: **incremental evaluation** means each new turn re-checks a growing conversation in milliseconds instead of re-verifying from scratch; **correct retraction** means an edited or truncated conversation history retracts exactly the conclusions that depended on the removed turns; **provenance** means a contradiction is never a vibe — it is a proof tree grounded in quoted spans of the user's own messages.

## 2. Motivation

Every serious LLM deployment eventually asks: *"did the model (or the user, or my own system prompt) just contradict itself?"* Today the only answer is "ask another LLM," which is unfalsifiable, unexplainable, and re-rolls the dice on every check. InputLayer can give a categorically better answer — deterministic rules over extracted facts, with receipts — but only if getting that answer requires zero integration work.

Hence the OpenAI-compatible surface. Changing `base_url` is the lowest-friction adoption path in the ecosystem; every framework (LangChain, LiteLLM, Vercel AI SDK, raw `openai` clients) supports it. The feature also dogfoods the engine end-to-end (facts, rules, recursion, vector search for entity resolution, retraction, proof trees) and gives the repo a demo that lands in thirty seconds: *send a conversation that contradicts itself, get back a proof of the contradiction.*

## 3. Goals and non-goals

**Goals**

1. Drop-in gateway: `ANTHROPIC_API_KEY` in env → `/v1/chat/completions` works with unmodified OpenAI clients.
2. Verify **internal consistency of the incoming conversation** (system + user + prior assistant turns) on every request. Verifying the *generated* output is a fast follow (M3), same machinery.
3. Findings are explainable: every conflict carries claim IDs, message indices, surface spans, and a structured proof tree.
4. Verification is incremental across turns of the same conversation and never blocks completions in the default mode (fail-open).
5. Rule packs are ordinary `.il` files a user can read, extend, and replace.

**Non-goals (for this RFC)**

- Fact-checking against world knowledge. This verifies *coherence*, not *truth*. (Checking against a user-supplied ground-truth KG is a natural later extension — same rules, different fact source.)
- Full first-order theorem proving, modal/counterfactual reasoning, or catching rhetorical/pragmatic contradictions.
- Training a local extraction model. The gateway starts with Claude as the extractor; a distilled local extractor is M4 and slots in behind the same interface.

## 4. User experience

### 4.1 Setup

```toml
# config.toml
[llm]
provider          = "anthropic"
api_key_env       = "ANTHROPIC_API_KEY"
completion_model  = "claude-sonnet-4-6"    # default forwarding target
extraction_model  = "claude-haiku-4-5"     # fast/cheap; extraction is a narrow task

[verify]
default_mode        = "annotate"           # annotate | enforce | off
verify_output       = false                # also check the generated completion (M3)
rule_packs          = ["consistency.core"]
session_ttl_minutes = 120
max_sessions        = 10000
```

```bash
docker run -p 8080:8080 -e ANTHROPIC_API_KEY=sk-ant-... ghcr.io/inputlayer/inputlayer
```

If no key is configured, the gateway routes are simply not mounted; the core engine is unaffected.

### 4.2 Calling it

```python
from openai import OpenAI
client = OpenAI(base_url="http://localhost:8080/v1", api_key="unused")

r = client.chat.completions.create(
    model="claude-sonnet-4-6",
    messages=[
        {"role": "system", "content": "You are a travel-planning assistant. The client's total budget is $2,000."},
        {"role": "user", "content": "We're flying out of Geneva on August 14th for the Kyoto conference. My brother Robert is coming too."},
        {"role": "assistant", "content": "Great — departing Geneva on the 14th. I'll plan around the conference dates."},
        {"role": "user", "content": "Since we leave on the 12th, can you make sure Bob gets an aisle seat?"},
    ],
)
print(r.choices[0].message.content)          # normal completion
print(r.model_extra["inputlayer"])           # verification findings
```

The `inputlayer` block on the response (schema in §9):

```jsonc
"inputlayer": {
  "consistency": {
    "status": "conflicts_found",            // verified | conflicts_found | unverified
    "session": "conv_9f3ba2",
    "messages_checked": 4,
    "facts": { "extracted_this_turn": 3, "total": 11 },
    "conflicts": [{
      "kind": "functional_conflict",
      "entity": "trip", "attribute": "departure_date",
      "values": ["2026-08-14", "2026-08-12"],
      "sources": [
        { "message": 1, "surface": "flying out of Geneva on August 14th" },
        { "message": 3, "surface": "since we leave on the 12th" }
      ],
      "proof": { /* structured .why tree, §7.3 */ }
    }],
    "latency_ms": { "extraction": 640, "reasoning": 4 }
  }
}
```

Nothing about the OpenAI-shaped part of the response changes; clients that ignore unknown fields keep working untouched.

### 4.3 Verify-only endpoint

`POST /v1/verify` accepts the same `{"messages": [...]}` payload, runs extraction + rules, and returns only the findings — no completion is generated. This is the cheap building block: lint a system prompt for self-contradictory instructions in CI, batch-audit stored conversations, or wire verification into a pipeline that uses a different LLM provider for generation.

### 4.4 Modes

`annotate` (default) attaches findings and never blocks. `enforce` returns HTTP `422` with the findings body when conflicts are found, without calling the completion model — useful when the caller treats a contradictory prompt as a programming error. `repair` (M3) feeds the proof trees back to the model as a system-side correction instruction and regenerates once before responding.

## 5. Architecture

```
                    ┌────────────────────────── InputLayer process ─────────────────────────┐
                    │                                                                       │
 OpenAI client ───► │  Gateway (axum)                                                       │
 /v1/chat/…         │   ├─► Session resolver ── prefix-hash match ──► per-conv KG           │
                    │   │        │ (new suffix messages only)         `_conv_<id>`          │
                    │   │        ▼                                        ▲                 │
                    │   ├─► Extractor ── Anthropic Messages API ──► facts │                 │
                    │   │    (claude-haiku, structured outputs,     +claim[...] etc.        │
                    │   │     temperature 0, prompt-cached spec)          │                 │
                    │   │                                                 ▼                 │
                    │   │                                    Rule packs (consistency.core)  │
                    │   │                                                 │                 │
                    │   │                                     ?conflict / .why proof trees  │
                    │   │                                                 │                 │
                    │   └─► Anthropic Messages API (completion) ──┐       │                 │
                    │                     (runs in parallel)      ▼       ▼                 │
                    │                                   Response assembler ───► client      │
                    └───────────────────────────────────────────────────────────────────────┘
```

Two calls leave the box per request — the completion and the extraction — and they run **concurrently**, because verifying the incoming conversation does not depend on the completion. Rule evaluation is local and sits in the engine's incremental-update regime (milliseconds), so in `annotate` mode the p50 added latency over a plain proxy rounds to zero: extraction on a fast model almost always finishes inside the completion's shadow.

**Placement in the repo.** The gateway lives as a separate workspace member (`gateway/`) that talks to the engine over the existing WebSocket API like any other client, rather than linking into the core. This keeps HTTP/LLM dependencies (`axum`, `reqwest`, retry logic) out of the engine crate, exercises the public API, and lets the gateway be developed, tested, and even deployed independently. It ships in the same Docker image and is spawned by the entrypoint when an API key is present. (Alternative considered: a `--features llm-gateway` module inside the server binary. Rejected for coupling; revisit if the WS hop ever shows up in profiles.)

## 6. Behind the scenes: translating a conversation into facts

This is the heart of the feature. The pipeline is **extract → canonicalize → assert**, and every stage preserves provenance so proofs can point back at text.

### 6.1 Relation schema

The session KG uses a small, fixed schema. Extraction never invents relation names — it fills these:

```
// Conversation structure
message(MsgIdx, Role, Hash)

// Atomic claims: one fact per row, decontextualized
claim(ClaimId, Entity, Attribute, Value)
claim_modality(ClaimId, Modality)      // asserted | negated | hedged | conditional | opinion | question
claim_source(ClaimId, MsgIdx, Surface) // verbatim span that grounds the claim

// Event ordering (temporal claims get their own relation)
before_claim(ClaimId, EventA, EventB)

// Deontic constraints, typically from the system prompt (checked in M3)
constraint(ConstraintId, Type, Arg)
constraint_source(ConstraintId, MsgIdx, Surface)

// Ontology helpers shipped with the rule pack, extensible by the operator
functional(Attribute)                  // single-valued attributes
disjoint(TypeA, TypeB)                 // mutually exclusive classes
```

Two design rules keep the verifier trustworthy. First, **modality gating**: rules only fire on `asserted` and `negated` claims. Hedges ("we might leave on the 12th"), conditionals, opinions, and questions are extracted — they're informative — but stay inert, so uncertainty in language never manufactures a false alarm. Second, **provenance is mandatory**: a claim without a surface span is dropped at ingestion. If the extractor can't quote it, we don't reason over it.

### 6.2 The extraction call

One call per *new* message batch (see §8 for why it's only the new suffix), using the Anthropic Messages API:

- **Model:** `extraction_model` (default `claude-haiku-4-5`) at temperature 0.
- **Structured outputs:** the request pins a JSON schema via `output_config.format` (`type: "json_schema"`), so malformed output is structurally impossible — no repair parsing, no retries on syntax. (GA on Haiku 4.5 and newer; see the structured-outputs page on `platform.claude.com` docs.)
- **Prompt caching:** the extraction instructions + schema + predicate vocabulary form a static prefix marked with `cache_control`, so per-request extraction cost is dominated by the new messages only.
- **Instructions (sketch, full text in Appendix A):** decompose into atomic claims; resolve pronouns within the visible window; normalize dates to ISO-8601 and quantities to canonical units; tag modality conservatively — *when unsure between `asserted` and `hedged`, choose `hedged`*; emit a verbatim `surface` span per claim; emit nothing for content that carries no factual commitment.

The conservative-modality instruction is the precision dial. Extraction errors should overwhelmingly become *missed* contradictions (silent) rather than *false* ones (trust-destroying).

### 6.3 Canonicalization

"Robert" in message 1 and "Bob" in message 3 must be one entity or the departure-date conflict is undetectable. Resolution is two-stage, and the second stage is where the engine's hybrid design pays off:

1. **In-window coreference** is handled by the extractor itself (the prompt includes the conversation so far as read-only context, so it emits `robert` for "Bob" when the link is obvious).
2. **Cross-turn resolution** for non-obvious cases uses the engine: each new entity mention is embedded and stored; a candidate merge is a vector-similarity hit over the session's existing entities *filtered by logical compatibility* (same `is_a` type, no conflicting identifying attributes) — similarity proposes, rules confirm, in one query. Merges assert `same_as(E1, E2)`, and claims are read through a canonical-representative rule rather than rewritten, so a wrong merge is retractable.

### 6.4 Worked example

Take the conversation from §4.2. Extraction over messages 0–1 yields (abbreviated):

```jsonc
{ "claims": [
  { "id": "c_m1_1", "entity": "trip",   "attribute": "departure_city", "value": "Geneva",
    "modality": "asserted", "msg": 1, "surface": "flying out of Geneva" },
  { "id": "c_m1_2", "entity": "trip",   "attribute": "departure_date", "value": "2026-08-14",
    "modality": "asserted", "msg": 1, "surface": "flying out of Geneva on August 14th" },
  { "id": "c_m1_3", "entity": "robert", "attribute": "is_a", "value": "person",
    "modality": "asserted", "msg": 1, "surface": "My brother Robert is coming too" } ],
  "constraints": [
  { "id": "k_m0_1", "type": "max_total_budget", "arg": "2000 USD",
    "msg": 0, "surface": "total budget is $2,000" } ] }
```

The gateway loads these into the session KG:

```
+claim[("c_m1_1","trip","departure_city","Geneva"), ("c_m1_2","trip","departure_date","2026-08-14"), ("c_m1_3","robert","is_a","person")]
+claim_modality[("c_m1_1","asserted"), ("c_m1_2","asserted"), ("c_m1_3","asserted")]
+claim_source[("c_m1_1",1,"flying out of Geneva"), ("c_m1_2",1,"flying out of Geneva on August 14th"), ("c_m1_3",1,"My brother Robert is coming too")]
+constraint[("k_m0_1","max_total_budget","2000 USD")]
```

Message 3 arrives on the next request. Only it is extracted (the prefix is cached, §8):

```jsonc
{ "claims": [
  { "id": "c_m3_1", "entity": "trip", "attribute": "departure_date", "value": "2026-08-12",
    "modality": "asserted", "msg": 3, "surface": "since we leave on the 12th" },
  { "id": "c_m3_2", "entity": "robert", "attribute": "wants_seat", "value": "aisle",
    "modality": "question", "msg": 3, "surface": "can you make sure Bob gets an aisle seat" } ] }
```

Note `c_m3_2`: the seat request is phrased as a question, so it's tagged `question` and will never trigger a rule — but "since we leave on the 12th" is a presupposition stated as fact, and the extractor is instructed to treat presuppositions as `asserted`. "Bob" resolved to `robert` (§6.3). Inserting `c_m3_1` is a single-fact incremental update; the engine re-derives only what it touches.

## 7. The rule pack: `consistency.core`

Shipped as a plain `.il` file in `rules/consistency/core.il`, loaded into each session KG at creation. Everything below uses the engine's `head <- body` rule syntax; the comparison and wildcard builtins (`!=`, `_`) should be confirmed against the builtin reference during implementation.

```
// Modality gates — the only two doors into the conflict rules
+active(C, E, A, V) <- claim(C, E, A, V), claim_modality(C, "asserted")
+denied(C, E, A, V) <- claim(C, E, A, V), claim_modality(C, "negated")

// R1 · Functional conflict: a single-valued attribute with two values
+conflict("functional", C1, C2) <-
    active(C1, E, A, V1), active(C2, E, A, V2),
    functional(A), V1 != V2

// R2 · Polarity conflict: the same proposition asserted and denied
+conflict("polarity", C1, C2) <-
    active(C1, E, A, V), denied(C2, E, A, V)

// R3 · Disjointness: one entity in two mutually exclusive classes
+conflict("disjoint", C1, C2) <-
    active(C1, E, "is_a", T1), active(C2, E, "is_a", T2), disjoint(T1, T2)

// R4 · Temporal cycle: 'before' is transitive; nothing precedes itself
+before(X, Y) <- before_claim(_, X, Y)
+before(X, Z) <- before(X, Y), before(Y, Z)
+temporal_cycle(X) <- before(X, X)

// Seed ontology (operator-extensible)
+functional[("departure_date"), ("departure_city"), ("age"), ("birth_date"), ("capital_of")]
+disjoint[("person","organization"), ("alive","deceased")]
```

R4 is the demonstration of why a reasoner belongs here at all: "A before B" in turn 2, "B before C" in turn 7, "C before A" in turn 12 is a contradiction that **no pair of sentences exhibits** — it only exists after recursive chaining, which is precisely the engine's native operation, and precisely what an LLM-as-judge reviewing sentence pairs misses.

### 7.1 What fires on the example

```
?conflict(Kind, C1, C2)

┌──────────────┬──────────┬──────────┐
│ Kind         │ C1       │ C2       │
├──────────────┼──────────┼──────────┤
│ "functional" │ "c_m1_2" │ "c_m3_1" │
└──────────────┴──────────┴──────────┘
```

### 7.2 The receipt

```
.why ?conflict("functional", "c_m1_2", "c_m3_1")
// [rule] conflict (clause 0): conflict("functional",C1,C2) <- active(C1,E,A,V1), active(C2,E,A,V2), functional(A), V1 != V2
//   [rule] active: active(C,E,A,V) <- claim(C,E,A,V), claim_modality(C,"asserted")
//     [base] claim("c_m1_2", "trip", "departure_date", "2026-08-14")
//     [base] claim_modality("c_m1_2", "asserted")
//   [rule] active: …
//     [base] claim("c_m3_1", "trip", "departure_date", "2026-08-12")
//     [base] claim_modality("c_m3_1", "asserted")
//   [base] functional("departure_date")
```

### 7.3 From proof tree to API response

The response assembler walks the proof tree, joins every `[base] claim(...)` leaf against `claim_source`, and emits the `conflicts[]` entry shown in §4.2 — kind from the rule head, values from the claim rows, message indices and verbatim spans from provenance, and the tree itself (as structured JSON, not rendered text) under `proof`. A finding is thus checkable by a human in seconds: two quoted spans and the rule that relates them.

### 7.4 Constraint rules (M3 preview)

Once output verification lands, the same machinery checks the *completion* against system-prompt deontics — extracted output-claims vs. `constraint` rows:

```
+violation("budget", C, K) <-
    output_claim(C, "itinerary", "total_price", V),
    constraint(K, "max_total_budget", Max),
    gt(V, Max)                     // requires numeric comparison builtins — see Open Questions
```

Constraints are a different logical species from claims (rules about what output *must* be, not what *is*), which is why they get their own relations rather than being shoehorned into `claim`.

## 8. Session lifecycle: statelessness meets incrementality

The OpenAI API is stateless — every request carries the full message list. The engine is incremental. The session resolver bridges the two:

1. Per request, compute a **chained hash** over messages: `h_i = H(h_{i-1} ‖ role_i ‖ content_i)`.
2. Look up a session KG whose stored hash chain is a prefix of the incoming one. Hit → extract and assert **only the suffix** (usually one user turn). Miss → new KG named `_conv_<h_n[:12]>`, extract everything once.
3. **Divergence** (client edited or truncated history at index k): retract all facts with provenance `MsgIdx >= k`, then extract the new suffix. This is where correct retraction earns its keep — a conflict derived through a now-deleted turn disappears, but a conclusion still supported by surviving turns stays, with no recompute and no stale flags.
4. Clients may pin sessions explicitly with an `il_conversation_id` field (or `X-IL-Conversation` header) to survive prompt-prefix rewrites (e.g., sliding-window truncation by a framework).
5. Eviction: TTL + LRU per `[verify]` config. An evicted conversation that returns is simply re-extracted in full — correctness is unaffected, only cost.

Net effect: steady-state per-turn work is *one Haiku extraction of one message* plus a *millisecond-scale incremental rule evaluation*, regardless of conversation length. Verification cost stops scaling with history — which is exactly the property that makes always-on checking viable.

## 9. API specification

### 9.1 Request

`POST /v1/chat/completions` accepts the standard OpenAI schema. Recognized standard fields (`model`, `messages`, `temperature`, `top_p`, `max_tokens`, `stop`, `stream`) are mapped onto the Anthropic Messages API (`max_tokens` required → default from config; `system` role → Anthropic `system` param). Unknown OpenAI fields are ignored, logged at debug. A `model` value beginning with `claude-` is forwarded verbatim; anything else falls back to `completion_model`.

Optional InputLayer extension object in the body (all fields optional):

```jsonc
"inputlayer": {
  "mode": "annotate",                 // annotate | enforce | off  (default from config)
  "conversation_id": "my-session-1",  // pin session identity
  "rule_packs": ["consistency.core"], // override packs for this session
  "verify_output": true               // M3: also check the completion
}
```

### 9.2 Response

Standard OpenAI chat-completion object, plus the top-level `inputlayer.consistency` block (§4.2). `status` semantics: `verified` (extraction succeeded, zero conflicts), `conflicts_found`, `unverified` (extraction or engine failure — carries a `reason`; the completion is still returned in `annotate` mode). In `enforce` mode, conflicts short-circuit before the completion call and return `422` with the same findings body.

### 9.3 Streaming

`stream: true` is passed through as SSE. Verification of the *incoming* conversation runs concurrently with generation; its findings are emitted as one extra SSE chunk immediately before `[DONE]`, carrying the `inputlayer` block on an otherwise-empty delta (clients that ignore unknown fields are unaffected). With `verify_output`, output extraction necessarily runs after the stream completes and adds one Haiku-call of tail latency to that final chunk — this is the documented cost of output checking in streaming mode.

### 9.4 `/v1/verify`

Request: `{ "messages": [...], "inputlayer": { ... } }`. Response: the `consistency` block alone. No completion model is invoked; only the extraction model is. This endpoint is also the natural CI tool: `il verify prompts/system.md` (thin CLI wrapper, M2) lints a system prompt for internally contradictory instructions before it ever ships.

## 10. Performance and cost budget

| Component | Latency (p50, typical turn) | Cost driver |
|---|---|---|
| Completion (unchanged) | seconds | operator's chosen model |
| Extraction (parallel) | ~0.4–1.0 s on Haiku | new-suffix tokens only; static prefix prompt-cached |
| Canonicalization | ms | embedding of new mentions only |
| Rule evaluation | ms (incremental) | engine-local |
| Response assembly | ms | proof-tree walk |

Because extraction runs in the completion's shadow, `annotate` mode adds ~zero wall-clock latency for prompt-side verification in the common case. Dollar cost is one small-model call per turn over just the new message — with prompt caching, typically a low-single-digit percentage of the completion's own cost. `enforce` mode inverts the ordering (verify first, complete second) and therefore does add extraction latency to the critical path; that trade is the point of the mode.

## 11. Failure modes and safeguards

**Fail-open by default.** Any extractor error (rate limit, timeout, refusal) or engine error yields `status: "unverified"` with a reason; the completion is never held hostage by the verifier in `annotate` mode.

**False-alarm discipline.** The three precision mechanisms are structural, not aspirational: modality gating (§6.1), conservative-modality extraction instructions (§6.2), and merge-by-rule canonicalization that keeps entity merges retractable (§6.3). The test suite (§12) gates CI on false-alarm rate, not just detection rate — a verifier that cries wolf gets `mode: off`'d within a week and the feature dies.

**Hallucinated extraction.** A claim the source text doesn't support is the residual risk. M4 adds an entailment guard (small NLI model checking claim-vs-surface-span) as an ingestion filter; until then the mandatory verbatim `surface` span keeps every finding human-checkable in seconds.

**Security and privacy.** The Anthropic key never leaves the server. Message contents are not logged by default (`[gateway] log_content = false`). Session KGs are namespaced per conversation and evicted per TTL; a `DELETE /v1/sessions/{id}` route allows explicit purge. Self-hosting remains the deployment story — conversation data never transits anything but the operator's box and the Anthropic API they configured.

**Prompt-injection containment.** Extracted values are data, never code: they enter the KG only through parameterized fact insertion over the fixed schema, so conversation text can't smuggle rules, queries, or commands into the engine.

## 12. Implementation plan

**M0 — Verify-only (target: ~2 weeks).** `gateway/` workspace member; `/v1/verify`; extraction with structured outputs + prompt caching; `consistency.core` (R1–R3); response assembly from proof trees. *Demo: paste a self-contradictory conversation, get a proof.*

**M1 — Proxy.** `/v1/chat/completions` non-streaming, `annotate` + `enforce`, parallel verification, OpenAI↔Anthropic param mapping. *Demo: change `base_url` in an existing app, nothing breaks, findings appear.*

**M2 — Incremental sessions + streaming.** Prefix-hash session resolution, suffix-only extraction, divergence retraction, SSE with trailing findings chunk, R4 (temporal), `il verify` CLI. *Demo: 40-turn conversation, per-turn verification cost flat.*

**M3 — Output verification.** `verify_output`, constraint extraction from system prompts, `violation` rules, `repair` mode (proof tree → correction instruction → single regeneration).

**M4 — Hardening & independence.** NLI entailment guard; optional local extractor (distilled per the training plan discussed separately) behind the same extractor trait; additional upstream providers if demanded.

**Testing.** A golden corpus of clean conversations plus a mutation harness that injects contradictions programmatically (flip a date, swap an entity, negate a claim, create a temporal cycle) — CI reports detection rate and false-alarm rate per rule; regressions on false alarms block merge. Engine-side rule tests are ordinary `.il` fixtures under `tests/`.

## 13. Open questions

1. **Builtins.** Exact spelling/availability of `!=`, numeric comparisons (`gt`), and wildcard `_` in rule bodies — R1 and the M3 budget rule depend on them. If numeric comparison over stored strings isn't available, values need typed columns or a normalization pass at ingestion.
2. **Negation semantics.** The pack deliberately avoids negation-as-failure (polarity is modeled as paired positive relations). Confirm this remains sufficient once constraint rules land, or document the engine's stratification story.
3. **Structured `.why` over the wire.** The response assembler needs the proof tree as JSON via the WS API, not rendered text. If only text exists today, a `proof_json` capability is a small prerequisite engine PR.
4. **Rule-pack loading.** Per-session KG import of `.il` packs vs. a shared read-only KG referenced by sessions — depends on engine support for cross-KG reads; per-session copy is the safe default.
5. **Embedding provider for canonicalization** — engine-internal embeddings vs. gateway-computed (e.g., via the configured LLM provider); affects the `[llm]` config surface.
6. **Multi-tenant quotas.** `max_sessions` is a blunt instrument; per-key quotas and per-KG memory ceilings may be needed before anyone puts this in front of untrusted traffic.
7. **Licensing note.** The gateway is a feature of the repo under the existing Apache-2.0 + Commons Clause terms; it consumes the *operator's* Anthropic key. No change proposed, flagged for completeness.

---

## Appendix A — Extraction prompt (sketch)

```
You convert conversation messages into atomic factual claims for a logic engine.

Output ONLY the JSON schema provided. For each claim:
- One fact per claim. Split conjunctions. Resolve pronouns using the conversation
  context. Use snake_case canonical entity ids; reuse ids from ENTITIES_SO_FAR.
- attribute: choose from VOCABULARY when possible; otherwise coin snake_case.
- Normalize dates to ISO-8601 (resolve relative dates against CURRENT_DATE),
  quantities to "<number> <unit>".
- modality: asserted | negated | hedged | conditional | opinion | question.
  Presuppositions stated as fact ("since we leave on the 12th") are asserted.
  When torn between asserted and hedged, ALWAYS choose hedged.
- surface: verbatim substring of the source message that grounds the claim. Required.
- Temporal orderings go to before_claims as {event_a, event_b}.
- System-message obligations/prohibitions/limits go to constraints, not claims.
- Emit nothing for greetings, meta-talk, or content with no factual commitment.
  An empty claims array is a correct output.

CURRENT_DATE: {{date}}
VOCABULARY: {{predicate_vocabulary}}
ENTITIES_SO_FAR: {{session_entities}}
MESSAGES_TO_EXTRACT: {{new_suffix}}
CONTEXT (read-only, do not extract): {{prior_messages}}
```

*Static blocks (instructions, schema, vocabulary) are marked with `cache_control` for prompt caching; only the last three template slots vary per call.*

## Appendix B — Response `conflicts[].proof` shape

```jsonc
{ "rule": "conflict/functional",
  "clause": "conflict(\"functional\",C1,C2) <- active(C1,E,A,V1), active(C2,E,A,V2), functional(A), V1 != V2",
  "children": [
    { "rule": "active", "children": [
      { "base": "claim(\"c_m1_2\",\"trip\",\"departure_date\",\"2026-08-14\")" },
      { "base": "claim_modality(\"c_m1_2\",\"asserted\")" } ] },
    { "rule": "active", "children": [
      { "base": "claim(\"c_m3_1\",\"trip\",\"departure_date\",\"2026-08-12\")" },
      { "base": "claim_modality(\"c_m3_1\",\"asserted\")" } ] },
    { "base": "functional(\"departure_date\")" } ] }
```
