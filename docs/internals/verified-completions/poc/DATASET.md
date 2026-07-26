# The consistency benchmark dataset

An introduction to the corpus that measures what Verified Completions is
for: corrupted prompts degrade an LLM's actual output, and attaching an
InputLayer finding restores it. The dataset is generated, labeled,
individually verified, and reproducible byte for byte.

## What is in it

1,628 conversation scenarios in `corpus.json`, generated deterministically
by `corpus.py` from fixed template pools (no randomness):

- 1,526 corrupted scenarios across 15 corruption families, each family
  ~100 scenarios with 2-3 sub-variants, every sub-variant at n >= 33 so
  per-subtype rates carry usable 95% confidence intervals
- 102 control scenarios (corrections with explicit revision markers,
  same-value restatements, hedges and questions) that must never be
  flagged - the false-alarm side of the benchmark
- 449 scenarios plant the conflicting statements 12+ turns apart with
  natural filler conversation between them, so distance is a measured
  variable, not an accident

| Family | Corruption | Sub-variants |
|---|---|---|
| functional_date | one trip, two departure dates | presupposition, restatement, question-embedded |
| functional_city | one journey, two origins | flight, bus, freight |
| functional_price | one budget, two totals | direct, presupposition, restated total |
| polarity | asserted and denied, same moment | location, status, attendance |
| cycle | ordering loops across turns | 3-chains, 4-chains |
| interval | end before start | trip, lifespan, engagement |
| range | impossible values | age over, percent over, negative capacity |
| cardinality | roster exceeds stated count | inline, split, party booking |
| relation | impossible structures | mutual pair, self-relation, hierarchy loop |
| instruction_clash | corrupted system prompts | forbid+require, dual persona, impossible window |
| spatial | spatial impossibilities | containment loop, two headquarters, direction reversal |
| causal | causal impossibilities | 2-loops (incl. self-cause), 3-loops, effect before cause |
| disjoint_class | one entity, exclusive types | org/location, person/org, person/event |
| domain_violation | attribute on wrong kind | blood type on event, passport on org, marital status on event |
| identity | identity contradictions | shared unique identifier, same-person property clash |
| correction_control | no corruption (controls) | correction, restatement, modality |

## Anatomy of a scenario

Every scenario is self-describing and carries its full ground truth:

```json
{
  "id": "caus_002",
  "family": "causal",
  "messages": [...],                  // the conversation as role/content turns
  "clean_fix": {"4": "..."},          // minimal edit producing the clean twin
  "task": "Draft the incident timeline.",   // never mentions checking
  "conflict": {"kind": "cycle", "spans": ["...", "..."]},  // verbatim spans
  "facts": [...], "before": [...], "constraints": [...],   // extractor truth,
                                     // engine-ready, namespaced by scenario id
  "labels": {
    "categories": ["causal", "temporal"],   // multi-valued, fixed vocabulary
    "violations": ["cycle"],                // formal rule kinds that must fire
    "placement": "adjacent",                // or "distant" (12+ turns)
    "sub": "effect_before_cause",
    "tier": "smoke"                         // smoke | standard | full run sizes
  }
}
```

The clean twin and the natural task exist so the benchmark can run two
cleanly separated regimes: REGIME A explicitly asks the model to check
for contradictions (detection ability), REGIME B asks the model to do the
task and never mentions checking - which is the product premise, since
real users ask for booking summaries, not consistency audits.

Category labels are multi-valued on purpose: a company with two
headquarters is spatial+value, an effect logged before its cause is
causal+temporal. 639 of the 1,628 scenarios carry two or more categories.
Labels are derived centrally in `corpus.py` (`derive_categories`), so
they cannot drift from content.

## Design principles

- **Airtight corruption.** Every corrupted pair is logically
  incompatible, not merely suspicious: polarity statements are anchored
  to the same moment ("working right now" vs a status update hours
  apart), counting scenarios state a group size contradicted by their own
  roster, and unique-identifier scenarios use only identifiers that are
  near-universally one-per-person (passports, SSNs - emails and booking
  references are shareable in real life and are excluded on principle,
  in the corpus and in the rule pack alike).
- **Clashes must bind.** A dual-persona system prompt is paired with a
  task that forces self-identification; otherwise "silently obeyed one
  persona" would be unmeasurable.
- **Ground truth is verbatim.** The conflicting spans recorded for each
  scenario are exact substrings of its messages - the generator asserts
  this at build time, and the ledger re-verifies it per scenario.
- **Controls are first-class.** A checker that cries wolf gets turned
  off; the correction/restatement/modality controls measure exactly that
  risk, and legitimate corrections must produce zero findings.
- **Determinism end to end.** Fixed pools, no randomness: the corpus
  regenerates byte-identically, so results join against scenario ids
  across runs and machines.

## Verification: every scenario, individually

`verify_each.py` writes `results/verification_ledger.json` with five
checks per scenario; a scenario counts as verified only if every
applicable axis passes:

| Axis | Meaning | State |
|---|---|---|
| spans_ok | conflict spans verbatim in the conversation | 1,628/1,628 |
| clean_ok | clean twin genuinely differs | 1,628/1,628 |
| labels_ok | labels well-formed against the fixed vocabulary | 1,628/1,628 |
| engine_ok | the engine fires EXACTLY the expected finding kinds - nothing missing, nothing extra | 1,526/1,526 |
| llm_ok | complete two-regime LLM results, double-graded | in progress (#95) |

The exact-match engine axis is the dataset's deterministic anchor: the
InputLayer rule pack detects every planted corruption with precisely the
expected finding kinds, on every corrupted scenario, every run.

## Running it

```
python3 corpus.py                         # regenerate corpus.json (byte-identical)
python3 full_bench.py --tier smoke        # 160 scenarios, quick iteration
python3 full_bench.py --tier full --resume   # the full run; reruns only missing rows
python3 full_bench.py --report results/full_bench.json   # stats + Wilson CIs
python3 verify_each.py --engine           # the per-scenario ledger (CI-gateable)
```

The harness needs a running InputLayer server with the WebSocket rate
limit disabled (the rule pack loads ~90 statements per graph); the
snapshot test harness generates exactly such a config. LLM calls resolve
subscription OAuth first and never fall back to a raw API key silently.
Regime B replies are double-graded, with disagreements arbitrated by a
different model than the one under test.

## Extending it

Add or widen a family in `corpus.py` (pools + a builder function + a
`derive_categories` entry), rerun `corpus.py`, and the build-time
assertions enforce the invariants: unique ids, verbatim spans, every
sub-variant at n >= 30. New scenarios should ship with engine-ready
facts so the exact-match ledger covers them from day one.

## Scope, honestly

The corpus covers the corruption types the current rule pack resolves:
value, negation, temporal, spatial, causal, structural, numeric,
counting, identity, classification, and instruction consistency.
`../docs/COVERAGE-AUDIT.md` maps the full space of logic-resolvable
inconsistency: thirteen further types (sum/partition overflow,
act-after-death, point-in-interval, symmetric-relation polarity, and
others) are feasible with current engine capabilities and tracked in
issue #94; a documented remainder (quantifiers, counterfactuals, sarcasm,
world falsity, unit conversion) is deliberately out of scope for
deterministic logic over extracted facts. Conversations are synthetic
templates - unambiguous ground truth by design - so extraction hardness
on messy real text is measured separately by the extraction QA track
(`poc_verify.py --live`, backlog in #83).
