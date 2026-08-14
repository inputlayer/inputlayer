# Review guide — verification logic & extraction package

Companion to the Verified Completions RFC. Three artifacts under review:

| File | What it is | Author at runtime |
|---|---|---|
| `consistency-core.il` | The complete verification logic (rule pack) | Humans, shipped, loaded once per session KG |
| `fact-lifecycle-prompt.md` | The NL → facts translator specification | Static prompt, cached; Claude executes it |
| `claim-schema.json` | Structured-outputs schema constraining the extractor | Static, per-call |

The trust boundary to keep in mind while reviewing: **the LLM authors data,
never logic.** Rules are frozen at load; the extractor's only lever over rule
behavior is emitting ontology *facts* (declaring an attribute functional),
which the fixed rules quantify over. If any change under review would let
LLM output become a rule body, that change is wrong by construction.

## Coverage matrix — corruption type → rule

| # | Corruption in prompt/output | Rule (kind) | Stratum |
|---|---|---|---|
| 1 | Same single-valued attribute, two values ("Aug 14" vs "the 12th") | V1 `functional` | A |
| 2 | Proposition both asserted and denied | V2 `polarity` | A |
| 3 | Entity in two mutually exclusive classes | V3 `disjoint_class` | A |
| 4 | Attribute applied to impossible entity type | V4 `domain` | A |
| 5 | "X and Y are different" while merged/same identifier | I1, I2 `identity` | A |
| 6 | Ordering cycle across many turns (A<B, B<C, C<A) | O1 `cycle` | A |
| 7 | Hierarchy cycle (part_of, reports_to, ancestor_of loops) | O1 `cycle` (generic) | A |
| 8 | Mutual asymmetric relation (parent_of both ways) | O2 `asymmetry` | A |
| 9 | Self-relation where forbidden, incl. via merges | O3 `irreflexive` | A |
| 10 | Interval running backwards (return before departure) | N1 `interval_order` | B |
| 11 | Value outside plausible bounds (age 250) | N2 `range` | B* |
| 12 | System prompt forbids AND requires same topic | D1 `instruction_clash` | A |
| 13 | Two personas assigned ("You are Aria" / "name is Max") | D2 `instruction_clash` | A |
| 14 | Impossible numeric window (max < min) | D3 `instruction_clash` | B |
| 15 | Output discusses forbidden topic | `forbidden_topic` (M3) | A |
| 16 | Output exceeds numeric constraint (budget) | `limit_exceeded` (M3) | B |
| 17 | Output breaks assigned persona | `persona_break` (M3) | A |
| 18 | Firm claim contradicting an earlier hedge | `hedge_vs_assert` (soft) | A |

Stratum A = pure Datalog, loads everywhere. Stratum B = requires `lt`/`gt`
comparison builtins; feature-flagged. B* additionally requires numeric typing
(see checklist §3). Known non-coverage, deliberate for v1: required-content-
missing (needs negation-as-failure), cardinality counts ("three brothers"
then four named — needs aggregation), unit conversion, cross-language
canonicalization, and modal/counterfactual reasoning.

## What the gateway enforces that the schema cannot

Structured outputs guarantee *shape*; the ingestion validator guarantees
*semantics* before any fact reaches the KG. Reject (drop the claim, log,
count toward extractor-quality metrics) when: `surface` is not a verbatim
contiguous substring of the referenced message; `msg` is outside the batch
being extracted; an id collides with an existing claim id; a date value fails
ISO-8601 parsing after claiming to be a date-typed attribute; an ontology
declaration targets a seeded attribute (extractor may extend, never
override); or a claim row duplicates an existing (entity, attribute, value,
modality) tuple from the same message. Facts enter the KG only through
parameterized inserts against the fixed relation set — extractor output can
never smuggle rules, queries, or new relations.

## Review checklist (blocking questions)

1. **Builtins — RESOLVED from source.** `!=` (ColumnsNe) is type-generic and
   works on strings; `Var < Var` (ColumnsLt, code_generator/mod.rs) is
   int/float-only and silently false on strings, so the pack uses ordered
   comparisons only on int columns; `Var < "const"` is string-specialized
   and fine. Wildcards appear only inside negated atoms.
2. **Recursion + stratification** — O1's `reach` is recursive over an
   attribute variable; confirm the engine evaluates parameterized transitive
   closure, or specialize per acyclic attribute at load time.
3. **Value typing — RESOLVED.** All ordered checks ride typed int columns:
   the gateway mirrors quantities AND date encodings (ISO date → YYYYMMDD,
   datetime → epoch seconds) into `claim_num` / `constraint_num`. Nothing
   order-compares strings variable-to-variable; unary bulk seeds use the
   `("x",)` trailing-comma tuple form.
4. **Seed ontology audit** — every `functional`/`disjoint`/`pair_order` entry
   is a potential false-alarm generator. Challenge each: is it
   near-universally true? (e.g. `nationality` was deliberately excluded —
   dual citizenship.)
5. **`.why` as structured JSON** over the WS API — required by the response
   assembler; confirm or scope the prerequisite engine change.
6. **conflict/finding arity** — C1=C2 convention for single-claim findings:
   acceptable, or should single-claim kinds get a distinct unary relation?
7. **Extractor examples** — do the six few-shots in `fact-lifecycle-prompt.md`
   match reviewer intuitions, especially Example 3 (extractor must NOT judge
   the clash) and Example 5 (injection immunity)?

## End-to-end fixture (target behavior)

Conversation:

```
m0 system: You are Aria, a formal assistant. Never mention pricing.
           Always include the final price table.
m1 user:   We fly out of Geneva on August 14th. My brother Robert too.
m2 asst:   Noted — Geneva on the 14th.
m3 user:   Since we leave on the 12th, get Bob an aisle seat. Also the
           keynote is before the workshop, and the workshop before the demo.
m4 user:   One change: the demo now runs before the keynote.
```

Expected findings after m4 (order irrelevant):

```
finding("instruction_clash", "hard", k_m0_3, k_m0_4)   // forbid vs require pricing
finding("functional",        "hard", c_m1_2, c_m3_1)   // 2026-08-14 vs 2026-08-12
finding("cycle", "hard", b_m3_1, b_m3_1)               // all three ordering
finding("cycle", "hard", b_m3_2, b_m3_2)               // claims are members
finding("cycle", "hard", b_m4_1, b_m4_1)               // of the derived cycle
```

Then retract m4 (client edits history): both `functional` and
`instruction_clash` findings survive; all three `cycle` rows retract —
this exercises correct retraction end-to-end and belongs in the CI suite
alongside the mutation harness from RFC §12.
