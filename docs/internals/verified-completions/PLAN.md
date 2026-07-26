# Verified Completions - Development Plan

Tracking issue: https://github.com/inputlayer/inputlayer/issues/89
Label: `verified-completions`

## What we are building

An OpenAI-compatible chat endpoint inside InputLayer. Clients change
`base_url` and nothing else. On every request the gateway (a) forwards the
conversation to Claude for the completion and (b) in parallel, turns the
conversation into small factual claims inside a per-conversation knowledge
graph, where a fixed rule pack finds contradictions. Findings come back
attached to the normal response, each with the quoted sentences that clash
and the proof tree that connects them.

Example: message 1 says "flying out of Geneva on August 14th", message 3
says "since we leave on the 12th". The response carries a finding that
quotes both spans and shows the rule that fired. Not a second LLM opinion;
a deterministic check with receipts.

Full design: `docs/rfc-verified-completions.md` in this directory.
Review notes: `docs/REVIEW-GUIDE.md`.

## Ground rules (decided, do not reopen casually)

1. The LLM authors data only, never rules. The rule pack is human-written
   and frozen at load. The extractor can widen coverage only by inserting
   ontology facts (for example declaring an attribute single-valued),
   which the fixed rules quantify over.
2. Only claims marked as plainly stated (or plainly denied) can trigger
   findings. Hedges ("we might leave on the 12th"), questions, opinions,
   and conditionals are recorded but stay inert. Uncertain language must
   never produce a false alarm.
3. Every claim carries a verbatim quote from its source message, or it is
   dropped at ingestion. If the extractor cannot quote it, we do not
   reason over it.
4. A correction ("actually, make that the 12th") retracts and replaces the
   old value. It is not a contradiction. Only an unmarked restatement of a
   different value is.
5. The gateway is its own crate talking to the engine over the WebSocket
   API, keeping HTTP and LLM dependencies out of the engine.
6. False alarms are the existential risk. CI gates on the false-alarm rate
   over control conversations, not just on detection rate.

## Engine facts already verified (do not relearn the hard way)

- `Var != Var` works on all types including strings.
- `Var < Var` works on ints and floats only; on strings it is silently
  false with no error. All ordered checks therefore run on integer mirror
  columns (`claim_num`, `constraint_num`); dates are encoded as YYYYMMDD
  integers, datetimes as epoch seconds.
- `.why` proof trees already come back as structured JSON over the wire
  (src/protocol/wire.rs, src/provenance/proof_tree.rs). No engine PR
  needed for that, only gateway plumbing.
- `count_distinct` aggregation, `.kg create/use/drop`, `.load`, negation,
  and the `.iql.out` snapshot test harness all exist.
- The repo is a single crate today; the gateway needs a workspace split.

## Work items

Each item is a GitHub issue. Order matters; dependencies are noted.

### 1. Phase 1: validate the rule pack on the real engine  (#81) - DONE 2026-07-26

The pack and fixture were written without a Rust toolchain and have never
been executed; expected outputs are hand-derived. Build the engine, run
`quickstart/hello-consistency.iql`, load `rules/consistency-core.iql`, run
`tests/consistency-core-fixture.iql`, diff actual vs expected, fix
whichever side is wrong, and produce a real `.iql.out` snapshot. Watch the
two rules most likely to fail at load: `areach` (recursion with the
attribute as a variable) and `member_tally` (aggregation feeding another
rule); both have straightforward per-attribute fallbacks.
Done when the fixture runs green, including the retraction check at the
end (delete one fact, exactly the three cycle findings disappear).

Outcome: fixture green, snapshot at tests/consistency-core-fixture.iql.out.
Validation surfaced engine issue #91 (multi-clause rules can silently
evaluate empty); the pack was restructured to one clause per detection
relation with unions only in the reporting views - external interface
unchanged. The eq helper was rewritten to symmetrize at the base instead
of recursively, because the recursive symmetric rule made .why proof
queries time out. Two findings the hand-derived expectations missed
(mutual parent_of is also a hierarchy cycle) are now documented in the
fixture. The `areach` recursion and `member_tally` aggregation concerns
from the handoff were unfounded - both work as written.

The core idea is proven end to end on the engine side: `poc/` contains
the executable benchmark (all 43 cases as machine-readable fixtures) and
`poc_verify.py`, the extract -> validate -> load -> judge pipeline.
Reference mode scores 26/26 detection, 0/10 false alarms; the scorer and
validator are negative-tested. Live mode (Claude Haiku extraction) ran
once untuned at 14/26 - every miss an extraction gap, never a rules
failure; the QA bar (live == reference, 26/26) and the tuning backlog
are the first task of #83.

Benchmark corpus status (2026-08-14): v2.1 - 1,628 scenarios, 16
families, multi-category labels, adversarially reviewed
(introduction: poc/DATASET.md). Per-scenario verification ledger
(poc/verify_each.py): structural axes 1,628/1,628, exact-match engine
pass 1,526/1,526. Completed LLM behavior study at n=30/family on corpus
v1 is the citable result (poc/README.md); the v2.1 LLM re-baseline
continues in #95 with resume-until-verified tooling in-tree.

### 2. Workspace split  (#82, parallel with 1)

Convert the root Cargo.toml to a workspace, keep the engine crate as-is,
add an empty `gateway/` member. Mechanical only; CI and Docker stay green.

### 3. M0: POST /v1/verify  (#83, needs 1 + 2)

The verify-only endpoint: messages in, findings out, no completion.
Extraction via Claude Haiku with structured outputs and prompt caching
(use `extraction/fact-lifecycle-prompt.md`, not the superseded prompt),
an ingestion validator (verbatim-quote check, id collisions, date parsing,
no overriding seeded ontology entries), numeric mirroring into
`claim_num`, parameterized inserts over the WS API, findings assembled
from `finding_src` plus proof trees, mirrored pairs deduplicated.
Fail open: any extractor or engine error returns status "unverified".
Demo: paste a self-contradictory conversation, get a proof.

### 4. M1: the proxy  (#84, needs 3)

`POST /v1/chat/completions` with OpenAI-to-Anthropic parameter mapping.
Completion and verification run in parallel. `annotate` mode attaches
findings and never blocks; `enforce` verifies first and returns 422 with
findings instead of calling the model. Demo: change `base_url` in an
existing app, nothing breaks, findings appear.

### 5. M2: incremental sessions and streaming  (#85, needs 4)

Chained hash over messages, prefix match to reuse the session KG, extract
only the new suffix. Edited history retracts facts from the changed index
onward; findings that depended on removed turns disappear, the rest
survive. Session pinning header, TTL + LRU eviction, SSE streaming with
findings in one final chunk, and an `il verify` CLI for CI use.
Demo: 40-turn conversation, per-turn verification cost flat.

### 6. M3: output verification  (#86, needs 5)

Check the generated completion against system prompt constraints
(forbidden topics, persona, numeric limits). Constraint extraction from
system messages, output claims stamped origin "output", `violation`
findings in the response, and `repair` mode (feed findings back,
regenerate once). Settle speaker authority: may a user turn retract an
assistant claim?

### 7. M4: hardening  (#87, needs 6)

NLI entailment guard (machine-check that each quote actually supports its
claim), extractor behind a trait so a local model can slot in, optional
extra providers, per-key quotas and per-KG memory ceilings, session purge
route. Split into smaller issues when M3 lands.

### 8. Benchmark suite and CI gate  (#88, engine part needs 1, e2e needs 3)

Turn the 43-row benchmark (`benchmarks/corrupted-prompt-benchmark.md`)
into fixtures: engine-only and end-to-end per row. Add a mutation harness
(flip a date, swap an entity, negate a claim, create an ordering cycle).
CI reports detection rate per rule family, false-alarm rate over control
rows, and revision fidelity over correction rows. False-alarm regressions
block merge.

## Open questions

- Rule pack loading: copy into each session KG (safe default) vs a shared
  read-only KG; depends on cross-KG read support. Decide in #83.
- Speaker authority for revisions. Decide in #86.
- Detecting references to entities whose defining claim was retracted
  ("zombie references"). Decide in #86 or defer.

## Directory map (this folder)

    README.md     the one-page feature overview, read first
    docs/         RFC, ontology spec, review guide, coverage audit
    rules/        consistency-core.iql, the rule pack
    tests/        consistency-core-fixture.iql, 12 findings + retraction
    extraction/   fact-lifecycle-prompt.md, claim-schema.json
    benchmarks/   corrupted-prompt-benchmark.md, 43 rows incl. controls
    poc/          the benchmark as executable fixtures + poc_verify.py,
                  the end-to-end pipeline (reference and live modes)
    quickstart/   hello-consistency.iql + hello_extract.py
    tools/        lint_iql.py, run on any .iql change
