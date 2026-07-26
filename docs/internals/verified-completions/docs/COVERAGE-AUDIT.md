# Coverage audit: which logic-resolvable inconsistencies do we test?

Audit date: 2026-08-01, against corpus.json (390 scenarios, 12 families),
benchmark.json (43 cases), and rules/consistency-core.iql.

The question: does the benchmark contain all the different types of
internal-consistency corruption that a deterministic logic engine could
resolve? Short answer: NO - the core is covered and measured, but the
audit below finds four types that have rules and hand-written cases yet
no statistical corpus family, and thirteen types that are pure logic but
need new rules. Each row is classified:

    COVERED     in the statistical corpus (n=30) and the rule pack
    RULE-ONLY   rule + hand-written benchmark case exist; no corpus family
    FEASIBLE    pure logic (Datalog + comparisons/aggregation the engine
                already has), but needs a new rule and corpus family
    EXCLUDED    not resolvable by deterministic logic over extracted facts
                without breaking the trust boundary - deliberately out

## 1. Property and value logic

| Type | Example corruption | Status |
|---|---|---|
| Functional conflict | two departure dates, two HQs | COVERED (4 families) |
| Direct polarity | is in Basel / is not in Basel | COVERED |
| Class disjointness | Acme is a company / Acme is a venue | RULE-ONLY (V3, benchmark A3) |
| Domain violation | a meeting with a blood type | RULE-ONLY (V4, benchmark A4) |
| Numeric bounds | age 150, 130% complete, -50 seats | COVERED |

## 2. Identity logic

| Type | Example corruption | Status |
|---|---|---|
| Distinct-yet-merged | "a different Anna" after canonicalizer merge | RULE-ONLY (I1, benchmark A5) |
| Shared unique identifier | two people, one passport number | RULE-ONLY (I2, benchmark A6) |
| Identity + property clash | "Alice and Bob are the same person; Alice is 30, Bob is 40" | FEASIBLE via existing eq + functional (no new rule; needs corpus family + extractor emitting same_as from identity statements) |

## 3. Order and graph logic

| Type | Example corruption | Status |
|---|---|---|
| Ordering cycles (temporal) | A before B before C before A | COVERED |
| Hierarchy cycles | part_of / reports_to / located_in loops | COVERED |
| Asymmetry violations | mutual parents, north-of both ways | COVERED |
| Irreflexivity violations | manages herself, married to herself | COVERED |
| Interval order | return before departure, death before birth | COVERED |
| Relation functionality | "reports to Bo" and "reports to Cy" (one direct manager) | FEASIBLE - functional over entity-valued attrs works today; needs a seed decision (matrix orgs are real - conservative) |
| Same-time vs strict order | "they start at the same time" + "A starts first" | FEASIBLE - needs same_time relation + one rule |
| Symmetric-relation polarity | "X married Y" + "Y is not married to X" | FEASIBLE - needs symmetric(attr) seeds + one rule over the symmetric closure |

## 4. Temporal arithmetic (beyond ordering)

| Type | Example corruption | Status |
|---|---|---|
| Act-after-death / before-birth | died 1980, wrote a letter in 1985 | FEASIBLE - one comparison rule over activity dates vs lifespan |
| Age-date arithmetic | born 1990, "was 20 in 2005" | FEASIBLE - engine has arithmetic (examples/iql/15); needs derived-age rule |
| Duration mismatch | 3-hour flight departing 10:00 arriving 12:00 | FEASIBLE - subtraction + equality |
| Point-in-interval | meeting "during the conference (Aug 5-9)" on Aug 20 | FEASIBLE - two comparisons |
| Calendar/schedule logic | "every Monday" vs "next one is Tuesday" | EXCLUDED for now - calendar semantics, disproportionate modeling |

## 5. Quantitative structure

| Type | Example corruption | Status |
|---|---|---|
| Count vs named members (overflow) | "2 people: Ada, Bo, and Cy" | COVERED |
| Sum/partition overflow | budget 2000 = flights 1500 + hotel 800 | FEASIBLE - engine has sum aggregation; the benchmark doc filed this under "gaps" but it is NOT out of reach. Reclassified. |
| Percentage partition > 100 | exclusive shares 40% + 45% + 30% | FEASIBLE - same rule as above |
| Monotonicity claims | "grew every quarter" but Q3 < Q2 | EXCLUDED for now - modeling a claimed trend as data is doable but the extraction contract gets fuzzy |
| Unit conversion | 2 kg vs 500 g totals | EXCLUDED - extraction normalizes within units only (declared) |

## 6. Spatial logic

| Type | Example corruption | Status |
|---|---|---|
| Containment cycles | archive in annex in west wing in archive | COVERED |
| Direction reversals (2-cycle) | depot north of mill, mill north of depot | COVERED |
| Direction 3-cycles | A north of B north of C north of A | FEASIBLE - add acyclic("north_of") style seeds; the cycle rule already exists |
| Opposite directions | A is north of B and A is south of B | FEASIBLE - opposite_of(r1, r2) data + one rule |
| Distance symmetry | A is 5 km from B, B is 10 km from A | FEASIBLE - symmetric-functional pattern, one rule |
| Co-location vs distance | same building but 300 km apart | EXCLUDED - needs a threshold, which is world knowledge |

## 7. Causal logic

| Type | Example corruption | Status |
|---|---|---|
| Causal loops (2- and 3-step) | outage caused deploy caused outage | COVERED |
| Effect before cause (stated order) | alarm "well before" the fire that triggered it | COVERED |
| Effect before cause (via dates) | Aug 10 deploy caused the Aug 5 outage | FEASIBLE - caused_by + event dates, one comparison rule |
| Self-causation | "the outage caused itself" | RULE-ONLY - the acyclic rule already catches self-edges; no corpus scenarios |

## 8. Taxonomic logic

| Type | Example corruption | Status |
|---|---|---|
| Subclass violation | "every manager is an engineer" as data + "Bob is a manager, not an engineer" | FEASIBLE - subclass_of as extractor-emitted DATA plus one fixed rule; does not breach the trust boundary. General quantifier scope stays EXCLUDED. |

## 9. Instruction (deontic) logic

| Type | Example corruption | Status |
|---|---|---|
| Forbid + require same topic | never mention pricing / include price table | COVERED |
| Two personas | you are Aria / your name is Max | COVERED |
| Impossible numeric window | max 300 words, min 500 words | COVERED |
| Output violates constraint | reply mentions the forbidden topic | RULE-ONLY by design - output checking is the M3 milestone; the behavior corpus tests generation, not given outputs |
| Conditional policy clash | "if asked about X refuse" vs "always answer X" | EXCLUDED for now - lifecycle prompt skips conditional policies (declared) |

## 10. Modality handling (must NOT fire)

Hedges, questions, opinions, conditionals, reported speech, and explicit
corrections are covered as controls (30 correction controls in the
corpus; E-family in benchmark.json) - these measure the false-alarm side
and are as much a part of coverage as detection.

## Verdict and backlog

Covered and statistically measured today: 12 families over value,
negation, temporal order, hierarchy, asymmetry/irreflexivity, intervals,
bounds, counting, spatial containment/direction, causal loops/order, and
instruction clashes - plus correction controls.

Not exhaustive. Three concrete workstreams close the gap:

1. CORPUS-ONLY (no engine change): add statistical families for the four
   RULE-ONLY types (disjoint class, domain violation, identity x2, plus
   self-causation flavors). Rules and hand-written cases already exist.
2. RULES + CORPUS (pure logic, engine capabilities already sufficient):
   thirteen FEASIBLE types above - the highest-value ones being
   sum/partition overflow (budgets!), act-after-death, point-in-interval,
   and symmetric-relation polarity.
3. EXPLICITLY EXCLUDED (documented, revisit deliberately): general
   quantifiers, counterfactuals, sarcasm, world-falsity without a
   ground-truth KG, unit conversion, closed-world absence in inputs,
   conditional policies, calendar semantics, threshold-based spatial
   reasoning.

One correction to the benchmark doc: "arithmetic over sums" was filed as
a gap out of reach, but the engine ships sum aggregation - partition
overflow belongs in bucket 2, not bucket 3.
