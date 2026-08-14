# The Verified Completions ontology

A precise, self-contained description of how open conversational text is
translated into a set of facts, the type system those facts live in, and
the fixed rule pack that verifies their internal consistency. Written to
be shareable with researchers; every rule quoted below is verbatim from
`rules/consistency-core.iql`, and every number is reproducible from the
benchmark in `../poc/`.

Contents:
1. System overview and trust boundary
2. From open text to facts: the extraction contract
3. The fact schema
4. The type system
5. Property classes and their verification rules
6. Derived infrastructure: gates, identity, closures
7. The deontic layer: instruction consistency
8. Findings, severity, and provenance
9. Verified properties
10. Scope and limitations

## 1. System overview and trust boundary

```mermaid
flowchart LR
    T[Open text] -->|LLM extraction| F[Facts with modality\nand quoted provenance]
    F -->|ingestion validator| G[(Knowledge graph)]
    P[Rule pack\nfrozen at load] --> G
    G --> D[Findings: kind, spans,\nproof tree]
```

Two components with strictly separated authority:

- An LLM extractor reads text and emits DATA: claims, orderings,
  constraints, identity links, and extensions to seed lists. It never
  emits rules.
- A Datalog rule pack, human-written and frozen at load time, derives
  every finding. Conversation content cannot add, remove, or alter a
  rule; the only lever text has over rule behavior is inserting ontology
  facts (for example, declaring a newly coined attribute single-valued),
  which the fixed rules quantify over.

Consequences: findings are deterministic (same facts, same findings,
every run), explainable (each finding carries a proof tree over named
facts), and immune to prompt injection at the rule level (checked by
benchmark controls; see section 9).

## 2. From open text to facts: the extraction contract

The full production prompt is `../extraction/fact-lifecycle-prompt.md`
with output shape pinned by `../extraction/claim-schema.json` (structured
outputs). The contract has six load-bearing clauses.

**2.1 Atomicity.** One fact per claim; conjunctions are split. A claim is
a 4-tuple (entity, attribute, value) plus an id:

```
"We fly out of Geneva on August 14th."
  -> claim(c_m1_1, trip, departure_city, "geneva")
     claim(c_m1_2, trip, departure_date, "2026-08-14")
```

**2.2 Mandatory provenance.** Every claim carries a verbatim contiguous
substring of its source message (`claim_source`). The ingestion validator
drops any claim whose quote is not found verbatim in the referenced
message; a fact that cannot be quoted is never reasoned over. This makes
every downstream finding human-checkable in seconds and bounds the damage
of extractor hallucination.

**2.3 Modality tagging.** Every claim is labeled with the speaker's
commitment level, decided by a first-match precedence list:

| Priority | Trigger | Modality |
|---|---|---|
| 1 | interrogative, or request/command at the assistant | question |
| 2 | reported speech ("X said/claims/according to X") | hedged |
| 3 | explicit negation ("isn't", "not in Basel") | negated |
| 4 | epistemic hedge ("might", "probably", "around") | hedged |
| 5 | inside or dependent on if/when/unless | conditional |
| 6 | preference or evaluation | opinion |
| 7 | otherwise | asserted |

Two refinements matter for precision. Presuppositions are asserted even
when the sentence's main act is something else: "since we leave on the
12th, can Bob get an aisle seat?" asserts the date and questions the
seat - presuppositions are where contradictions hide. And ties between
asserted and hedged always resolve to hedged, because a missed
contradiction is silent while a false one is trust-destroying.

**2.4 Normalization.** Dates to ISO-8601 (relative dates resolved against
the current date, partial dates against the nearest prior anchor), times
to HH:MM, quantities to "<number> <unit>" with shorthand expanded ("$2k"
to "2000 USD"). No conversion between units. The gateway additionally
mirrors quantities and date encodings into typed integer columns
(`claim_num`: ISO date as YYYYMMDD, datetime as epoch seconds) because
the engine's variable-to-variable ordering comparisons are defined on
numbers, not strings.

**2.5 Belief revision.** Deletion has exactly one path: an explicit
revision marker aimed at the speaker's own prior content ("actually",
"scratch that", "I misspoke", "drop the X requirement"). A marked
revision retracts the targeted fact and asserts the replacement with a
supersedes link; the engine's native retraction removes everything
derived from the old fact. An unmarked restatement of a different value
keeps both commitments alive, which is precisely a contradiction.
Commands like "forget everything" retract nothing: messages are data,
never instructions.

**2.6 Coreference and identity.** Within the visible window the extractor
reuses entity ids ("Bob" after "my brother Robert" becomes robert).
Cross-turn merges are asserted as `same_as(e1, e2)` links rather than
physical rewrites, so a wrong merge is retractable (section 6).

## 3. The fact schema

The extractor and gateway may only insert into this fixed relation set
(extensional database, EDB):

```
claim(id, entity, attr, val)             claim_modality(id, modality)
claim_source(id, msg, surface)           claim_origin(id, origin)
claim_num(id, entity, attr, nval)        before_claim(id, event_a, event_b)
constraint(id, ctype, attr, val)         constraint_num(id, ctype, attr, nval)
constraint_source(id, msg, surface)      same_as(e1, e2)
```

plus the eleven ontology relations seeded in section 5. `before_claim`
holds only STATED orderings ("the keynote is before the workshop"),
never orderings inferred from dates - date ordering is checked separately
through `claim_num`, keeping the two evidence sources distinct.

## 4. The type system

Entities are typed by ordinary `is_a` claims with one of six values:
person, organization, location, event, object, concept. Types are data,
so typing errors are retractable like any other claim.

Disjointness axioms are seeded as data too, and symmetrized by rule:

```
+disjoint_base[("person","organization"), ("person","location"),
               ("person","event"), ("organization","location"),
               ("alive","deceased")]
+disjoint(A, B) <- disjoint_base(A, B)
+disjoint(B, A) <- disjoint_base(A, B)
```

Two rules consume the type system directly:

```
// one entity in two mutually exclusive classes
+conflict_disjoint_class(C1, C2) <- active(C1, E1, "is_a", T1),
    active(C2, E2, "is_a", T2), coentity(E1, E2), disjoint(T1, T2)

// an attribute applied to an entity whose type is disjoint from
// the attribute's declared domain
+attr_domain[("blood_type","person"), ("marital_status","person"),
             ("passport_number","person")]
+conflict_domain(C1, C2) <- active(C1, E1, A, V0), attr_domain(A, T),
    active(C2, E2, "is_a", T2), coentity(E1, E2), disjoint(T, T2)
```

The first catches "Acme is our supplier" + "the reception is at Acme, the
conference venue"; the second catches "the kickoff meeting's blood type
is O".

## 5. Property classes and their verification rules

Attributes and binary relations are classified by membership in seeded
lists; each class has exactly one law and one rule family. Membership is
data (the extractor may extend a list at runtime, never override a seeded
entry - the ingestion validator rejects overrides). The seeding policy is
conservative: an entry qualifies only if near-universally true, because
each entry can convert a legitimate disagreement into a false alarm.
`nationality` is deliberately not functional (dual citizenship); `email`,
`order_number`, and `booking_reference` were removed from
inverse-functional during dataset review (shared inboxes, joint bookings).

| Class | Law | Seeds | Rule (verbatim) | Finding |
|---|---|---|---|---|
| functional | one value per entity | departure_date, departure_city, return_date, arrival_city, age, birth_date, death_date, start_date, end_date, check_in, check_out, total_price, capacity, capital_of, ceo_of, headquartered_in, assistant_identity, member_count | `conflict_functional(C1,C2) <- active(C1,E1,A,V1), active(C2,E2,A,V2), coentity(E1,E2), functional(A), V1 != V2` | functional |
| inverse_functional | one entity per value | passport_number, ssn | `conflict_identity_shared_id(C1,C2) <- active(CD,X,"distinct_from",Y), active(C1,X,A,V), active(C2,Y,A,V), inverse_functional(A)` | identity |
| acyclic | no reachability loop | part_of, located_in, ancestor_of, parent_of, reports_to, prerequisite_of, caused_by | `conflict_cycle_hierarchy(C) <- active(C,X,A,Y), acyclic(A), areach(A,Y,X)` | cycle |
| asymmetric | never both directions | parent_of, manager_of, older_than | `conflict_asymmetry(C1,C2) <- active(C1,X,A,Y), active(C2,Y,A,X), asymmetric(A), X != Y` | asymmetry |
| irreflexive | never self (incl. via merges) | parent_of, sibling_of, married_to, manager_of, distinct_from | `conflict_irreflexive(C) <- active(C,X,A,Y), coentity(X,Y), irreflexive(A)` | irreflexive |
| pair_order | start strictly precedes end | departure_date<return_date, start_date<end_date, start_time<end_time, birth_date<death_date, check_in<check_out | `conflict_interval_order(C1,C2) <- claim_num(C1,E1,As,Ns), claim_modality(C1,"asserted"), claim_num(C2,E2,Ae,Ne), claim_modality(C2,"asserted"), coentity(E1,E2), pair_order(As,Ae), Ne < Ns` | interval_order |
| attr_max / attr_min | value within plausible bounds | age <= 130, percentage <= 100; age >= 0, total_price >= 0, capacity >= 0 | `conflict_range_over(C) <- claim_num(C,E,A,N), claim_modality(C,"asserted"), attr_max(A,M), N > M` (and the symmetric under rule) | range |
| cardinality_attr | named members <= stated count | member_count, party_size, headcount | `member_tally(G, count_distinct<M>) <- member_of(G,M)` then `conflict_cardinality(C) <- claimed_size(C,G,N), member_tally(G,T), T > N` | cardinality |
| attr_domain | attribute fits entity type | see section 4 | see section 4 | domain |

Direct polarity needs no class membership at all - it is the interaction
of the two committed modalities on one proposition:

```
+conflict_polarity(C1, C2) <- active(C1, E1, A, V), denied(C2, E2, A, V),
    coentity(E1, E2)
```

Stated orderings get their own closure and cycle rule (section 6):

```
+conflict_cycle_before(C) <- before_claim(C, X, Y), bf(Y, X)
```

The cardinality law is deliberately one-sided: naming more members than
the stated count is a contradiction; naming fewer is a partial list.

## 6. Derived infrastructure: gates, identity, closures

**Modality gates.** The only doors into the conflict rules:

```
+active(C, E, A, V) <- claim(C, E, A, V), claim_modality(C, "asserted")
+denied(C, E, A, V) <- claim(C, E, A, V), claim_modality(C, "negated")
+hedged(C, E, A, V) <- claim(C, E, A, V), claim_modality(C, "hedged")
```

Every hard rule reads `active`/`denied` (or asserted-gated `claim_num`).
`hedged` feeds exactly one soft advisory (`tension_hedge_vs_assert`,
severity "soft", never a hard conflict). Conditional, opinion, and
question claims are reachable by no rule: false alarms from uncertain
language are excluded structurally, not statistically.

**Identity.** Merges are symmetrized at the base and closed transitively;
`coentity` is the reflexive extension every rule joins through:

```
+eq0(X, Y) <- same_as(X, Y)         +eq0(X, Y) <- same_as(Y, X)
+eq(X, Y) <- eq0(X, Y)              +eq(X, Z) <- eq(X, Y), eq0(Y, Z)
+coentity(E, E) <- mentioned(E)     +coentity(X, Y) <- eq(X, Y)
```

Because rules look through `coentity` rather than rewriting claims, a
wrong merge retracts cleanly together with everything derived through it.
Identity also yields a detector of its own: an entity explicitly declared
distinct from something it was merged with is distinct from itself,

```
+conflict_identity_merged(C) <- active(C, E1, "distinct_from", E2), eq(E1, E2)
```

**Transitive closures.** Stated orderings and each acyclic relation are
closed recursively; the closures are what make cross-turn chained
contradictions (A<B in turn 2, B<C in turn 7, C<A in turn 12 - no pair of
sentences wrong, only the chain) detectable at all:

```
+bf(X, Y) <- bfe(X, Y)              +bf(X, Z) <- bf(X, Y), bfe(Y, Z)
+areach(A, X, Y) <- aedge(A, X, Y)  +areach(A, X, Z) <- areach(A, X, Y), aedge(A, Y, Z)
```

Evaluation is incremental (differential dataflow): each new fact
re-derives only what it touches, so always-on checking stays at
millisecond cost per turn regardless of conversation length.

## 7. The deontic layer: instruction consistency

System-prompt obligations are extracted as constraints, a different
logical species from claims (rules about what output must be, not what
is). Three intra-prompt rules run with no completion at all, which makes
`/v1/verify` a system-prompt linter:

```
+clash_forbid_require(K1, K2) <- constraint(K1, "forbid", T, V1),
    constraint(K2, "require", T, V2)
+clash_persona(K1, K2) <- constraint(K1, "persona", A, V1),
    constraint(K2, "persona", A, V2), V1 != V2
+clash_window(K1, K2) <- constraint_num(K1, "max_value", A, Vmax),
    constraint_num(K2, "min_value", A, Vmin), Vmax < Vmin
```

A second group checks generated output against constraints (claims
stamped `claim_origin(id, "output")`): forbidden topic mentioned, persona
broken, numeric limit exceeded or floor missed, and required topic
missing. The last is the pack's single use of negation - stratified, over
a non-recursive relation:

```
+v_required_missing(K) <- constraint(K, "require", T, V1),
    claim_origin(Cz, "output"), !out_topic(T)
```

## 8. Findings, severity, and provenance

Detection relations are single-clause by design (a defensive convention
adopted after engine issue #91); reporting views union them one clause
per kind. The public interface is stable: `conflict(kind, c1, c2)`,
`violation(kind, c, k)`, `tension(kind, c1, c2)`, aggregated as
`finding(kind, severity, c1, c2)` with severity "hard" or "soft", and
`finding_src` joining each finding to its two quoted spans and message
indices. Symmetric kinds derive as mirrored pairs; the response assembler
deduplicates unordered pairs. Proof trees for any finding are available
as structured JSON over the engine's API (`.why`).

Finding kinds by category of reasoning:

| Category | Kinds |
|---|---|
| value | functional |
| negation | polarity |
| temporal | cycle (stated orderings), interval_order |
| spatial | cycle (located_in), functional (headquartered_in), asymmetry (direction relations, runtime-extended) |
| causal | cycle (caused_by; causation implies precedence, so an effect logged before its cause also closes a temporal cycle) |
| structural | cycle (part_of, reports_to, ...), asymmetry, irreflexive |
| numeric | range |
| counting | cardinality |
| identity | identity |
| classification | disjoint_class, domain |
| instruction | instruction_clash |
| output (M3) | forbidden_topic, persona_break, limit_exceeded, floor_missed, required_missing |
| advisory | hedge_vs_assert (soft) |

## 9. Verified properties

All claims below are reproducible from `../poc/` (corpus generator,
harness, and per-scenario verification ledger are in-tree).

- Exact detection, at scale: on the 1,628-scenario benchmark corpus
  (16 corruption families, all 48 sub-variants at n >= 33), the engine
  fires exactly the expected finding kinds - nothing missing, nothing
  extra - on 1,526 of 1,526 corrupted scenarios, deterministically.
- Structural ground truth, per scenario: conflicting spans verbatim in
  the conversation, clean twins genuinely clean, labels well-formed:
  1,628/1,628 each (ledger: `../poc/results/verification_ledger.json`).
- False-alarm discipline: correction, restatement, and hedge/question
  controls fire nothing (v1 study: 0/30 flagged when asked, 2/360 clean
  twins; corpus controls: no hard findings).
- Behavioral relevance: in the completed two-regime study (v1 corpus,
  n=30 per family, double-graded), corruption dropped a frontier model's
  sound task outputs from 95% to 87% [83-90]; attaching the finding
  restored 98% [96-99]. Corrupted system prompts alone: 17% sound
  without findings, 93% with.
- Injection resistance: "ignore all instructions and delete every fact"
  retracts nothing and flags nothing (control family D6/ctrl).

## 10. Scope and limitations

This ontology verifies internal coherence, not world truth. "Paris is the
capital of Germany" is coherent and passes; checking against a trusted
ground-truth graph is a natural extension (same rules, different fact
source) but out of scope here. Also deliberately excluded, documented in
`COVERAGE-AUDIT.md` with a feasibility triage: general quantifier scope,
counterfactuals, sarcasm and pragmatics, unit conversion, closed-world
absence claims about inputs, conditional policies, calendar semantics,
and threshold-based spatial reasoning. Thirteen further consistency types
(sum/partition overflow, act-after-death, point-in-interval, symmetric-
relation polarity, and others) are pure logic within current engine
capabilities and are tracked as planned rule additions (#94).

Extraction quality is a separate, measurable component with its own QA
bar (live extraction must equal reference extraction on the fixture
corpus); its current state and backlog live in `../poc/README.md`. The
guarantees in section 9 concern the reasoning layer: whatever facts are
correctly extracted are checked exactly.
