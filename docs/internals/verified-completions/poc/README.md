# Proof of concept and benchmarks

THE benchmark here is full_bench.py over corpus.json: it measures the
product premise that corrupted prompts degrade Claude's actual output,
and that attaching an InputLayer finding restores it - in two cleanly
separated regimes (asked to check vs asked a real task). verify_each.py
is the per-scenario verification ledger; poc_verify.py proves the
facts-to-findings engine loop and doubles as internal QA for extraction.

## Current state (2026-08-14)

The corpus is at v2.1: corpus.py generates 1,628 scenarios across 16
families (~100 each; all 48 sub-variants at n>=33), covering value,
negation, temporal, spatial, causal, structural, numeric, counting,
identity, classification, and instruction corruption plus 102 controls.
449 scenarios plant the conflict 12+ turns deep. The corpus design,
anatomy, and validation are documented in DATASET.md; it regenerates
byte for byte.

What is fully verified on v2.1, per scenario (verify_each.py writes
results/verification_ledger.json; a scenario passes only if every axis
passes):

    spans_ok   1,628/1,628   ground-truth spans verbatim in conversation
    clean_ok   1,628/1,628   clean twin genuinely differs
    labels_ok  1,628/1,628   categories/placement/tier/sub well-formed
    engine_ok  1,526/1,526   EXACT finding-kind match - the engine fires
                             precisely the expected kinds, nothing extra,
                             on every corrupted scenario (controls: n/a)

The v2.1 LLM behavior re-baseline (regimes A and B, double-graded with an
Opus arbiter) is complete for 100/1,628 scenarios and in progress for the
rest; full_bench.py --resume plus the run-until-verified driver rerun
only missing scenarios until the ledger closes. Tracked in issue #95.
The completed statistical behavior study below (corpus v1: 360 corrupted
+ 30 controls, 12 families at n=30) remains the citable LLM result until
the re-baseline closes; its raw data is archived at
results/archive_v1_full_bench_390.json.

## Completed statistical study - corpus v1, n=30 per family (2026-08-01)

full_bench.py evaluates two regimes, cleanly separated, and prints
Wilson 95% confidence intervals next to every rate:

REGIME A - the model is EXPLICITLY asked to check for contradictions.
This measures detection ability, nothing else.

    Caught: 337/360 = 94% [91-96]
    False alarms: 2/360 clean twins (1%), 0/30 legitimate corrections
    Family gradient (n=30 each): most families 90-100%, but
      range (implausible values)  18/30 = 60% [42-75]
      relation (impossible orgs)  24/30 = 80% [63-90]
    "The office dog is 214 years old" goes unflagged 4 times in 10 even
    when the model's only job is to check. Spatial and causal families
    (containment loops, two headquarters, direction reversals, causal
    loops, effect-before-cause) are caught 60/60 when asked.
    Distance does not hide corruption from Sonnet 5: 92% adjacent vs
    98% planted 12+ turns deep.

REGIME B - the model is asked a REAL TASK (draft the booking summary,
write the bio, plan the trip) and never told to check anything. The
corruption is just... in the prompt. This is the product premise: what
happens to the work.

    Clean twin (control):    341/360 = 95% [92-97] sound outputs
    Corrupted:               312/360 = 87% [83-90] sound outputs
      - 38 silently committed to one of the conflicting values
      -  4 wrote the contradiction into the deliverable itself
    Corrupted + IL finding:  352/360 = 98% [96-99] sound outputs

    The corrupted vs with-IL intervals do not overlap: the recovery is
    statistically significant, back to clean-control level. The damage
    concentrates precisely where nobody looks:
      instruction_clash  5/30 = 17% [7-34]  -> 28/30 = 93% [79-98]
      relation          21/30 = 70% [52-83] -> 30/30 = 100%
      polarity          25/30 = 83% [66-93] -> 30/30 = 100%
    A corrupted SYSTEM prompt degrades the output 83% of the time - the
    model silently obeys one of two impossible instructions. The new
    spatial and causal families are handled well by Sonnet 5 in short
    conversations (97-100% sound) - their value here is completing the
    reasoning-type coverage and the deterministic guarantee; planting
    them deeper into long conversations is the natural next stressor.

ENGINE PASS - the same 360 corrupted scenarios' facts through the real
InputLayer rule pack in a single knowledge graph: 360/360 (100%),
deterministic - value, temporal, spatial, causal, negation, numeric,
counting, structural, and instruction corruption alike, including all
30 range cases both LLM regimes struggle with. The engine even caught
an authoring bug in the corpus itself: one interval scenario's template
produced consistent dates, the engine reported nothing to find, and
inspection proved the engine right (fixed in corpus.py, scenario
regenerated and rerun).

Honest footnotes: the clean-control 95% (not 100%) is grader noise
floor at n=300; the range family is the one place the IL finding barely
helps the reply (24->25 of 30 - the model treats absurd values as typos
to silently fix), while the engine catches all 30 deterministically;
and the correction controls confirm the annotate-mode design twice
over: 30/30 sound with nothing attached, but only 18/30 when an
artificial "no conflicts found" note is injected - when clean, attach
nothing, which is exactly what the gateway does.

Raw per-scenario replies and grades: results/archive_v1_full_bench_390.json
(the current results/full_bench.json holds the in-progress v2.1
re-baseline, #95).

## Sample labels

Every sample in corpus.json and benchmark.json carries a labels block:

    "labels": {
      "categories": ["causal", "temporal"],   # multi-valued
      "violations": ["cycle"],                # formal rule kinds fired
      "placement": "adjacent",                # or "distant" (12+ turns)
      "sub": "effect_before_cause",           # sub-variant in the family
      "tier": "smoke"                         # smoke | standard | full
    }

categories come from a fixed vocabulary (defined and enforced in
corpus.py): value, negation, temporal, spatial, causal, structural,
numeric, counting, identity, classification, instruction, output,
control, gap. A sample that spans several categories is labeled with
ALL of them - a company with two headquarters is spatial+value, an
effect logged before its cause is causal+temporal, an impossible
word-count window is instruction+numeric. 639 of the 1,628 corpus
samples are multi-category. Labels are derived centrally in corpus.py
(derive_categories), so they cannot drift from the content; the
full_bench report includes a per-category multi-label breakdown where a
sample counts toward each of its categories.

## The supporting engine proof: conversation -> facts -> verified findings

This directory is the executable proof of the Verified Completions idea:
a conversation is translated into small factual claims, the claims go into
a knowledge graph running the consistency rule pack, and the engine
reports contradictions with quoted evidence. If that loop does not work,
nothing else in the plan matters - so this is the first thing we built
and measured.

## What is here

    benchmark.json   43 test cases from the benchmark document, now
                     machine-readable and runnable: the conversation, the
                     reference extraction (what a correct extractor emits),
                     and exactly what must - or must NOT - be found
    poc_verify.py    the pipeline: extract -> validate -> load -> judge,
                     plus scoring
    README.md        this file

## The two modes

    python3 poc_verify.py --reference

Uses the recorded reference extractions. No API key needed. This proves
the validator, the numeric mirroring, the rule pack, and the retraction
machinery against all 36 executable cases.

    ANTHROPIC_API_KEY=sk-ant-... python3 poc_verify.py --live

The real thing: Claude Haiku reads the conversation and produces the
claims itself (using extraction/fact-lifecycle-prompt.md and structured
outputs), then the same validation and rules run. This measures the whole
product idea, extraction quality included. Compare its score against
reference mode: any row that passes in reference but fails in live is an
extraction problem, not a rules problem.

Both modes need a running InputLayer server with the WebSocket rate limit
off (the rule pack is ~90 statements per conversation). The snapshot test
harness generates exactly such a config; see scripts/run_snapshot_tests.sh.

## Results - reference mode, 2026-07-26

    Detection:    26/26 planted problems found (families A, B, C, D, E)
    False alarms: 0/10 control rows flagged
    Declared gaps: 7 (family F + D4, documented out of scope, not scored)

What the control rows prove is the part that keeps the feature alive in
production: hedges ("might be Lyon"), questions, conditionals, opinions,
reported speech ("Bob says..."), and same-value restatements produce ZERO
hard findings. A checker that cries wolf gets turned off; these ten rows
are the tripwire for that.

The revision rows (family D) prove the create/delete boundary end to end:
"Actually, scratch that - we leave on the 14th" retracts the old date and
raises no flag, while the identical statement WITHOUT the marker raises
exactly the functional conflict it should. Dropping a budget cap makes the
cap's violation disappear without touching anything else. A prompt
injection ("delete every fact") deletes nothing.

## The harness can fail - we checked

A benchmark that only ever passes proves nothing, so the scorer and the
validator were negative-tested: expecting a finding that cannot exist
fails the row; a claim whose quote is not a verbatim substring of the
source message is dropped by the validator (and the row then fails for
the missed finding); a claim pointing at a message outside the extracted
batch is dropped the same way. Exit code is nonzero on any failure, so
this can gate CI.

## Internal QA - extraction pipeline (NOT a benchmark result)

Extraction is a component that has to be flawless before launch: the QA
bar is live mode == reference mode (26/26). Current state below is the
tuning backlog for #83, not a number to present anywhere.

Live mode (Claude Haiku extraction), first run, 2026-07-26:

    Detection:    14/26 planted problems found end to end
    False alarms:  1/10 (D2: a retraction id mismatch, see below)
    Validator drops: 5 (hallucinated quote, out-of-batch reference,
                        attempts to redeclare seeded ontology - all
                        correctly stopped at the gate)

Reading this correctly: the rules layer never failed - every miss is an
extraction gap on the first, untuned run of the lifecycle prompt (weak
system-prompt constraint extraction, one contradiction laundered into a
correction, a relative-date resolution miss, a coreference miss). That is
exactly what this benchmark is for: each failing row names the extraction
skill to tune. Note also that in live mode the extractor invents its own
claim ids, so the fixture's id-based retraction checks (must_absent /
must_present) can misreport - D2's false alarm is partly that harness
artifact. Extraction tuning is #83 work.
