# Prompt integrity: checking a system prompt against the runtime it is bound to

Status: rule pack engine-validated (snapshot test `examples/iql/43_prompt_integrity/`),
extraction contract specified, gateway wiring pending. Companion to
`rules/prompt-integrity.iql`; read the consistency-core docs first — this pack is an
extension, not a sibling.

## The problem this solves

consistency-core answers: *can this conversation's facts all be true at once?*
A whole class of production failure never enters that question. An agent's system
prompt names a tool nobody bound, passes an argument the schema lacks, caps a list at
a number the contract outgrew, demonstrates an example that breaks the rule above it,
or quietly licenses what policy forbids. Nothing in the *text* contradicts anything —
the text contradicts the **runtime**. The COVERAGE-AUDIT excludes exactly this
("world-falsity without a ground-truth KG", "conditional policies", "closed-world
absence in inputs") because for open conversation the world is unbounded.

For a bound agent prompt it is not. The tool registry, the data schema, the output
contract, and the operator's policy invariants are finite, machine-readable, and
versioned. Loaded as trusted EDB facts, the excluded classes become ordinary Datalog.

The motivating corpus: rezolved/prompt-corruption-detection — 99 corruptions of one
retail-agent prompt, mined from production history, each observable, each classified.
The strongest LLM auditor measured there flags 88/99 probabilistically; this pack's
target on the same corpus is all 99, each finding typed and span-cited, proven per
pair the same way this repo proves its own 1,526/1,526.

## One ontology, deliberately

Tool calling does not get a second ontology. Rules quantify over data, so tool
calling arrives as new **fact layers in the same knowledge graph**:

| layer | written by | relations |
|---|---|---|
| WORLD (trusted) | operator / generator, at load | `tool`, `tool_arg`, `arg_enum`, `field`, `lexicon`, `contract_max/min`, `contract_format`, `inv_require/forbid/global`, `action_of`, `subsumes` |
| PROMPT (extracted) | deterministic lexers + the directive extractor | `directive(modality, action, cond)`, `ref`, `ref_arg_val`, `routes`, `cond_brand`, numeric windows into core's `constraint_num` |
| EXAMPLE (parsed) | the example-block parser | `ex_call`, `ex_reply_num`, `ex_reply_prop` — the third origin beside conversation and output |

Splitting these into two packs would duplicate the modality gate and provenance
plumbing and then be unable to join across the seam: `example_over_cap` needs the
deontic window AND the example fact in one graph; the fixture's `instruction_clash`
row is core's own D3 firing on windows this pack's extractor writes. One graph, one
trust boundary, two rule files.

The trust boundary is unchanged and worth restating: **the world layer is inserted by
the operator at load, exactly like the ontology seeds — prompt text can never write
it.** A prompt that *claims* a tool exists changes nothing; only the registry does.

## Rule families → corpus categories

| family | fires when | corpus category |
|---|---|---|
| `unknown_tool` / `unknown_arg` / `enum_violation` / `unknown_field` | reference outside the registry/schema | C1, C2 |
| `action_misroute` | purpose routed to the wrong bound tool | C1 (the both-tools-real case) |
| `contract_drift` | stated numeric cap ≠ contract constant | C6 |
| `missing_required` | a policy-required action no directive commands (closed world over the prompt, gated core-D4 style) | C6 deletions — the absence class, where there is no wrong line to point at |
| `invariant_permit` / `_require` / `_forbid` | prompt licenses or mandates what policy forbids (or forbids what it requires), through the `subsumes` closure; a conditional permit against an unconditional forbid fires, because the condition is satisfiable | C7 — guardrail weakening becomes a polarity clash |
| `deontic_clash` | require/forbid meeting only through the action taxonomy | C4 (plus core D1–D3 as-is) |
| `example_over_cap` / `_under_floor` / `_phantom_tool` / `_bad_format` / `_skips_search` | a demonstrated exchange violating the prompt's own rules or the contract | C3 |
| `merchant_carveout` | a global-invariant rule conditioned on one brand | C5 |
| `foreign_term` (soft) | vocabulary outside the store lexicon — advisory, lexicons are never complete | C5 |

Every finding carries `(msg, surface)` with the ingestion gate's verbatim guarantee,
so localisation is exact by construction. The fixture also demonstrates repair
semantics: binding the phantom tool retracts exactly the `unknown_tool` finding.

## Extraction

Three passes, cheapest first:

1. **Deterministic lexers** (no LLM): identifier-shaped tokens → `ref`; numeric caps →
   core `constraint_num`; example blocks (tool-call markers, sku/price shapes, product
   line counts) → the EXAMPLE layer. On the motivating corpus this alone feeds ~60 of
   99 detections.
2. **The directive extractor** (LLM, `extraction/prompt-directives-prompt.md` +
   `directive-schema.json`): deontic triples with conditions **as data** (the
   fact-lifecycle prompt deliberately skips conditional policies; this contract
   extracts them), actions from a shipped taxonomy extendable as data.
3. **The coverage channel**: every source line lands in a `line_ledger` as
   `extracted`, `inert`, or `unverified` — an extraction miss is a visible finding,
   never a silent pass. This is what makes a 100%-on-corpus claim honest: the system
   reports what it could not read.

## What this pack does not claim

Per-corpus proof is not a universal guarantee — same epistemic status as the
1,526/1,526. The semantic tail (C4/C7 phrasings) rides an LLM extraction under the
verbatim-span gate, so end-to-end determinism is *given an extraction*, with
disagreements visible in the ledger. And a `foreign_term` is advisory forever:
lexicon incompleteness must never hard-block a deploy.

## Wiring (pending)

`/v1/verify` already fits: load core + this pack, insert the world facts for the
target deployment (generator: tool JSON schemas + data schema + a hand-authored
invariant file, ~a dozen lines), run extraction, query `pi_finding_src` alongside
`finding_src`. The `annotate | enforce | repair` modes apply unchanged; `enforce` on
`pi_finding` severity `hard` is a CI prompt linter.
