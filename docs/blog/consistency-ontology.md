# A gentle introduction to the consistency ontology

Every long conversation with an AI slowly builds up a little world. You mention
that you fly out of Geneva on August 14th. A few messages later your brother
Robert joins the trip. Twenty messages later someone types "since we leave on
the 12th, can Bob get an aisle seat?" and nobody notices that the world just
broke. The model keeps chatting happily, picks one of the two dates, and the
travel agent books the wrong flight.

This is not a rare failure. When we benchmarked it, a corrupted prompt like
that degraded the model's actual output in a measurable share of tasks, and
the worst part was the silence: the reply does not say "I am confused about
the date". It just commits to one value and moves on.

InputLayer's Verified Completions feature exists for exactly this moment. The
short version: translate the conversation into small facts, put the facts
into a knowledge graph, and let a deterministic logic engine check whether
they can all be true at the same time. If they cannot, you get a finding with
receipts - the two quoted sentences that clash and the rule that connects
them.

But short versions do not build understanding. So instead of describing the
system, let us build it together, from nothing, and watch every part of it
become necessary. By the end you will have reinvented Verified Completions
yourself, and its ontology will feel less like a specification and more like
a set of scars.

## The journey begins: just ask the model?

Your first instinct is the obvious one. If the conversation might contain a
contradiction, ask the model to check for contradictions. It is right there,
it read everything, surely it can look.

We measured exactly this. Asked directly, with checking as its only job, a
frontier model catches most planted contradictions - but not all of them, and
the misses are not random. Told that grandpa turns 150 this year, it writes
the birthday invitation. Told that Mia manages herself, it drafts the org
page. And it re-rolls the dice every time you ask: same conversation, two
runs, two different answers. Detection by vibes is real, but it is not
something you can build a guarantee on.

Worse, in real life nobody asks. The user asks for a booking summary, not a
consistency audit. Whatever checking happens has to happen on the side,
every turn, without being invited. That is the design constraint that shapes
everything else: the checker must be always on, cheap, and boring.

So we take checking away from the model's mood and give it to a machine that
never has moods. But a logic engine cannot read paragraphs. Which forces our
first real decision.

## Step one: write the world down

If the engine cannot read sentences, the sentences have to become rows.
Small, atomic, unambiguous rows:

```
claim(c1, trip, departure_date, "2026-08-14")
claim(c2, trip, departure_date, "2026-08-12")
```

The language model is genuinely great at this part - reading text and saying
what it commits to. And we add one rule that will pay for itself a thousand
times over: every fact must carry a verbatim quote of the sentence it came
from. No quote, no fact. This means any alarm the system ever raises can be
checked by a human in seconds, because the evidence travels with the row.

Look at those two rows, though. Nothing has been judged yet. There is no
error message. Two dates sit in a graph, perfectly comfortable. A database
does not know that a trip cannot leave twice.

Somebody has to teach it. That body of teaching is the ontology, and here is
the good news you discover early: it fits on a page.

## Step two: the first law

What is actually wrong with those two rows? Not the values. Either date is a
fine date. What is wrong is that departure_date is the kind of attribute
that holds one value at a time. So you write your first law, and it is
almost embarrassingly small:

if the same entity has two different asserted values for a single-valued
attribute, that is a conflict.

Logicians call such attributes functional. You do not need the word; you
need the list. A trip has one departure date and one departure city. A
person has one age, one birth date. A company has one headquarters, one CEO.
You write down eighteen of these and you have caught the Geneva bug from the
introduction with a rule you can read out loud.

And immediately you learn the discipline that will follow you through this
whole journey. Should nationality go on the list? It feels single-valued.
It is not - dual citizenship exists - and if your checker ever flags a real
person's real passport situation as a contradiction, trust dies on the
spot. Every entry must pass the same test: is this near-universally true?
The ontology grows one honest entry at a time, and stays small because
honesty keeps it small.

## Step three: the false alarm that almost kills the project

You ship your one-law checker into a test conversation and it immediately
embarrasses you. Earlier the user said "we fly out of Geneva". Then the
assistant mused "it might be Lyon we leave from, let me confirm". Two
departure cities, one functional attribute - your checker screams
contradiction.

But nothing is wrong. "Might be" is not a commitment. Neither is "would it
be crazy to leave on the 12th instead?", nor "I love that we depart early",
nor "if the flight is delayed, we leave on the 15th". Human language is
mostly maybes, and a checker that cannot tell a maybe from a statement is
worse than no checker at all, because people will turn it off within a week.

So you invent the second load-bearing idea: every fact gets tagged with how
strongly the speaker committed to it. Asserted. Negated. Hedged.
Conditional. Opinion. Question. And then the crucial move - only asserted
and negated facts are allowed anywhere near the conflict rules. Everything
else is stored, quoted, and deliberately inert.

This is a firewall, not a filter. A hedge cannot cause a false alarm because
the door it would need does not exist. When we later threw a hundred trick
controls at the finished system - hedges, questions, opinions, reported
speech, polite restatements - it raised zero hard findings. That silence is
architecture, not luck.

The negated tag quietly gives you a second law for free: the same
proposition asserted and denied is a conflict. "The venue is in Basel" and
"the venue isn't in Basel" now catch each other.

## Step four: Bob is Robert

Next embarrassment. Message 1: "my brother Robert is coming too". Message
3: "can Bob get an aisle seat?" Your facts say robert has one seat
preference and bob has another and no law connects them, because your graph
thinks they are two people.

Names are slippery and you cannot fix that with a law about attributes. You
need identity: a way to record that two entity names refer to one thing.

```
same_as(robert, bob)
```

Then you make every rule look through identity links when comparing
entities. You resist the tempting shortcut of physically merging the rows,
and this caution is rewarded almost immediately: merges can be wrong, and
when one is wrong you want to retract the link and have everything derived
through it disappear cleanly. Keeping identity as data, not surgery, makes
that possible.

Identity also hands you a delightfully weird new law. If the conversation
says "that's a different Anna, by the way" but your graph already merged
the two Annas, then the merged entity is now recorded as distinct from
itself. An entity distinct from itself is absurd, so the absurdity becomes
a detector - it means either the text or the merge is wrong, and a human
should look.

## Step five: people change their minds

"Actually, scratch that - we leave on the 14th after all."

Your checker sees the 12th and the 14th and prepares to celebrate another
catch. But nothing is contradictory here. The user corrected themselves.
This is the moment your system either learns manners or becomes a pedant
nobody deploys.

The fix lives in the translation step. When a sentence carries an explicit
revision marker - actually, correction, I misspoke, make that - the old
fact is retracted and the new one takes its place. The conflict that
briefly existed vanishes with the retracted fact, because the engine
handles retraction natively; nothing needs to be recomputed from scratch.

The same sentence without a marker stays a contradiction. "We leave on the
12th" followed later by a flat "we leave on the 14th" keeps both
commitments alive, and flagging that is correct. The line between
correction and contradiction sits exactly where your intuition puts it,
and now it is written down.

## Step six: the contradiction nobody can see

Everything so far, a careful human could have caught by rereading. Now
comes the case that justifies the whole machine.

Message 2: the keynote is before the workshop. Message 7: the workshop is
before the demo. Message 12: the demo comes before the keynote, right
after lunch.

Read any two of those sentences together and they are fine. No pair is
wrong. Only the chain is wrong, and the chain only exists if something
walks it: keynote before workshop before demo before keynote means the
keynote precedes itself, which no schedule survives.

Walking chains is called transitive closure, and it is the native gait of
a logic engine. It follows every ordering as far as it goes, at any depth,
across any number of turns, in milliseconds, incrementally as each new
fact arrives. This is the aha moment of the journey: for pairwise clashes
the engine is merely more reliable than the model, but for chained clashes
it is doing something the model, reviewing sentence pairs, structurally
misses.

And once you have the no-loops law, you notice it is not about time at
all. Org charts must not loop: Design inside Engineering inside Design is
nonsense. Physical containment must not loop: the archive inside the annex
inside the west wing inside the archive. Causes must not loop: the outage
caused by the deploy that was caused by the outage. One law, one rule
family, and suddenly your checker speaks temporal, spatial, structural,
and causal - because they were all the same shape underneath.

## Step seven: filling in the law book

From here the journey becomes pleasantly repetitive: meet a new absurdity,
notice its shape, add a small list.

Some relations are one-way streets. If Ada is Bo's parent, Bo is not
Ada's. If the depot is north of the mill, the mill is not north of the
depot. Some relations never point at yourself: nobody manages herself or
is her own sibling. Some pairs must be ordered: departure before return,
birth before death, check-in before check-out. A practical note gets
learned here the hard way - the engine compares these as plain integers,
dates encoded like 20260814, because comparing dates as text is quietly
treacherous.

Some values have limits. Age stays within 0 and 130, percentages do not
exceed 100, rooms do not hold negative people. This is the humblest list
and, in our measurements, the one the model needs most: language models
are strangely forgiving of impossible numbers, and the bounds list does
not forgive.

Counts must match rosters. "The team is 2 people: Ada, Bo, and Cy" names
three for a claimed size of two, so the engine counts. Fewer named than
counted is fine - lists can be partial. More is a contradiction.

And things have kinds. Every entity gets a type - person, organization,
location, event - and some types exclude each other. That catches Acme
being your supplier in one sentence and the building you are standing in
three sentences later, and it catches attributes landing on the wrong kind
of thing entirely: a kickoff meeting with a blood type, a committee with a
passport number. One last flip: some values identify their owner, so two
people who insist they are different cannot share a passport number. You
keep that list brutally short - passports and social security numbers -
after almost shipping emails on it and remembering in time that families
share inboxes.

## Step eight: the assistant has laws too

One evening you read a system prompt that says "never mention pricing" and,
four lines later, "always include the final price table", and you realize
you have been checking only half the conversation.

Instructions contradict each other exactly like facts do, and this turns
out to be the most damaging corruption of all, because the model does not
argue with its own system prompt. In our behavior benchmark, corrupted
instructions silently degraded the output more often than any factual
corruption - the model just obeys one side, and the prompt's author never
finds out. So instructions become facts too: forbid clashes with require,
one persona attribute holds one name, a maximum must sit above a minimum.
Your checker now lints system prompts before a single token is generated.

## Step nine: who gets to write laws

One question has been hiding under the whole journey. The language model
translates text into facts. Could a malicious conversation trick it into
translating text into rules?

No, and this is the boundary that makes the system trustworthy: the model
only ever writes data. Claims, orderings, constraints, identity links, and
additions to the seed lists - "visa_number is single-valued" is just
another row. The rules that decide what counts as a contradiction are
written by humans and frozen before the first message arrives.
Conversation text can no more add a law than a web form can rewrite your
database schema.

This is also why the results repeat. On our benchmark corpus of 1,628
generated conversations, the reasoning layer detects every planted
contradiction, every time, with zero false alarms on the controls. Not a
good day - a computation. Ask twice, get the same answer twice.

## What you built

Look back at the trail. You started with two innocent rows in a database
and were forced, one embarrassment at a time, into: facts with mandatory
quotes, a modality firewall, identity links, retraction, transitive
closure, and a one-page book of laws - single-valued attributes, one-way
and never-self relations, ordered pairs, bounds, counts, types, unique
identifiers, and instruction rules. That page is the ontology. Nothing on
it is clever. Everything on it is earned.

And when it fires, you do not get a vibe. You get receipts:

```
functional conflict on trip.departure_date
  "flying out of Geneva on August 14th"   (message 1)
  "since we leave on the 12th"            (message 3)
```

That is what Verified Completions is about. The model stays what it is
good at being: fluent. The graph stays what it is good at being: right.
And the checker you just built in your head sits next to the model you
already use, always on, quoting its evidence.

If you want to compare your mental version against the real one, the
ontology lives in
`docs/internals/verified-completions/rules/consistency-core.iql` as
readable rules, the translation contract in
`docs/internals/verified-completions/extraction/fact-lifecycle-prompt.md`,
and the benchmark that keeps it all honest in
`docs/internals/verified-completions/poc/`.
