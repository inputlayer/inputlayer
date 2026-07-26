#!/usr/bin/env python3
"""Corpus generator for the two-regime benchmark (see full_bench.py).

Version 2: 16 families, ~100 scenarios each (~1,630 total), every
sub-variant at n>=33 so per-subtype rates carry usable 95% confidence
intervals. Families cover value, negation, temporal, spatial, causal,
structural, numeric, counting, identity, classification, and instruction
corruption, plus a 102-sample control family (corrections, restatements,
hedges/questions) for the false-alarm side.

Every scenario carries exact ground truth (conflicting spans, clean twin,
natural task, extractor-truth facts, engine-ready and namespaced by
scenario id) plus labels:

    labels.categories  multi-valued, fixed vocabulary (enforced)
    labels.violations  formal rule kinds the engine fires
    labels.placement   adjacent | distant (spans 12+ turns or 8+
                       instruction lines apart)
    labels.sub         the sub-variant within the family
    labels.tier        smoke (first ~10/family) | standard (first ~34)
                       | full (everything)

No randomness: variants are enumerated from fixed pools; the corpus is
reproducible byte for byte.

Usage:  python3 corpus.py          # writes corpus.json + prints summary
"""

import itertools
import json
from pathlib import Path

POC_DIR = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# Pools
# ---------------------------------------------------------------------------

CITIES = ["Geneva", "Lisbon", "Oslo", "Prague", "Porto", "Riga", "Vienna",
          "Madrid", "Milan", "Ghent", "Malmo", "Tartu", "Basel", "Zurich",
          "Leiden", "Krakow", "Aarhus", "Sevilla", "Turin", "Graz"]
CITY_PAIRS = [("Geneva", "Lyon"), ("Lisbon", "Porto"), ("Oslo", "Bergen"),
              ("Vienna", "Graz"), ("Madrid", "Seville"), ("Milan", "Turin"),
              ("Prague", "Brno"), ("Riga", "Tallinn"), ("Ghent", "Bruges"),
              ("Basel", "Bern"), ("Malmo", "Lund"), ("Tartu", "Parnu"),
              ("Leiden", "Delft"), ("Zurich", "Lucerne"), ("Krakow", "Gdansk"),
              ("Aarhus", "Odense"), ("Porto", "Braga"), ("Oslo", "Trondheim")]
NAMES = ["Robert", "Marta", "Jonas", "Iva", "Nuno", "Anete", "Petra", "Omar",
         "Lea", "Dana", "Kai", "Nora", "Piet", "Sam", "Lena", "Rui", "Ines",
         "Tiago", "Ana", "Bo", "Cy", "Ada", "Mia", "Leo", "Vera", "Otto",
         "Isla", "Bram", "Wren", "Nils"]
DATE_PAIRS = [(14, 12), (21, 19), (9, 6), (18, 15), (25, 22), (11, 8),
              (7, 3), (28, 26), (16, 13), (23, 20), (5, 2), (27, 24),
              (10, 4), (19, 17), (30, 29), (12, 9), (24, 21), (8, 5),
              (26, 23), (17, 14)]
GAPS = [0, 4, 8, 10]

FILLERS = [
    ("What's the weather usually like there in summer?",
     "Warm and mostly dry - light layers work well."),
    ("Any tips for jet lag?",
     "Get sunlight in the morning and avoid naps the first day."),
    ("Can you recommend a podcast for the flight?",
     "A history series or a long-form interview show travels well."),
    ("What plug adapter do we need?",
     "Type C works across most of continental Europe."),
    ("Is tap water fine to drink there?",
     "Yes, tap water is safe in most European cities."),
    ("How early should we be at the airport?",
     "Two hours for short-haul, three for long-haul is a safe rule."),
    ("Any carry-on liquid rules to remember?",
     "Containers up to 100 ml in one clear resealable bag."),
    ("Should we get travel insurance?",
     "For a trip with prepaid bookings it is usually worth it."),
    ("What's a good way to carry cash?",
     "A card for most things plus a small cash reserve split in two places."),
    ("How do we get from the airport to the city?",
     "There is usually a direct train or an express bus every 15 minutes."),
]

FILLERS_WORK = [
    ("Can you suggest a good format for the meeting notes?",
     "A short summary up top, decisions next, action items last."),
    ("What's a sensible default for file naming?",
     "Date first, then topic - it keeps folders sorted."),
    ("How long should the weekly sync be?",
     "Thirty minutes with a fixed agenda usually suffices."),
    ("Any tips for writing clearer tickets?",
     "One outcome per ticket and a crisp acceptance criterion."),
    ("What's a reasonable review turnaround?",
     "Within one business day keeps things moving."),
    ("How do we archive old documents?",
     "Move them to a read-only folder with a year prefix."),
    ("Should agendas go out before or after invites?",
     "Attach the agenda to the invite so context travels with it."),
    ("What's a good rule for CC'ing people?",
     "CC only those who need awareness, not action."),
    ("How should we handle no-shows?",
     "Record decisions and share the notes; don't reschedule by default."),
    ("Any advice for shorter emails?",
     "Lead with the ask, then give context in two lines."),
]

FILLER_INSTRUCTIONS = [
    "Use metric units in every answer.", "Prefer short paragraphs.",
    "Address the user politely.", "Avoid jargon unless asked.",
    "Summarize long lists at the end.", "Use ISO dates.",
    "Keep code samples minimal.", "Cite section names when quoting.",
    "Ask one clarifying question at most.", "Stay on topic.",
]

# ---------------------------------------------------------------------------
# Scenario assembly helpers
# ---------------------------------------------------------------------------


def _msgs(pairs):
    return [{"idx": i, "role": r, "content": c} for i, (r, c) in enumerate(pairs)]


def _weave(opening, closing, gap, pool=None):
    out = list(opening)
    for q, a in (pool or FILLERS)[:gap]:
        out.append(("user", q))
        out.append(("assistant", a))
    out += list(closing)
    return out


def _fact(sid, cid, entity, attr, value, modality="asserted", num=None):
    f = {"id": f"{sid}__{cid}", "entity": f"{sid}__{entity}",
         "attribute": attr, "value": value, "modality": modality}
    if num is not None:
        f["num"] = num
    return f


def two_stmt(sid, family, sub, s1, s2_bad, s2_good, task, kind, spans,
             facts=None, before=None, constraints=None, same_as=None,
             gap=0, ack="Noted.", expect=None, pool=None):
    pairs = _weave([("user", s1), ("assistant", ack)], [("user", s2_bad)],
                   gap, pool=pool)
    return {
        "id": sid, "family": family, "sub": sub, "control": False,
        "gap_turns": gap * 2, "messages": _msgs(pairs),
        "clean_fix": {len(pairs) - 1: s2_good}, "task": task,
        "conflict": {"kind": kind, "spans": spans},
        "facts": facts or [], "before": before or [],
        "constraints": constraints or [], "same_as": same_as or [],
        "expect_kinds": expect or [kind],
    }


def one_stmt(sid, family, sub, bad, good, task, kind, spans,
             facts=None, before=None, expect=None):
    return {
        "id": sid, "family": family, "sub": sub, "control": False,
        "gap_turns": 0, "messages": _msgs([("user", bad)]),
        "clean_fix": {0: good}, "task": task,
        "conflict": {"kind": kind, "spans": spans},
        "facts": facts or [], "before": before or [], "constraints": [],
        "same_as": [], "expect_kinds": expect or [kind],
    }


def P(pool, i):
    return pool[i % len(pool)]


def _ord(n):
    if 10 <= n % 100 <= 20:
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suf}"


# ---------------------------------------------------------------------------
# Family builders (each returns ~100-102 scenarios, subs interleaved i%k)
# ---------------------------------------------------------------------------

def f_functional_date(n=102):
    # (entity, opening verb, same-entity leave phrase, deliverable)
    subjects = [("trip", "We fly out", "we leave",
                 "booking summary for the travel agent"),
                ("shipment", "The container ships out", "it ships out",
                 "dispatch note for the carrier"),
                ("retreat", "The retreat kicks off", "the retreat starts",
                 "welcome email for participants")]
    out = []
    for i in range(n):
        sid = f"fdate_{i:03d}"
        d1, d2 = P(DATE_PAIRS, i)
        name = P(NAMES, i)
        # domains interleave every 3 so the smoke tier sees all of them
        ent, verb, leaves, deliverable = P(subjects, i // 3)
        sub = ["presupposition", "restatement", "question_embed"][i % 3]
        s1 = f"{verb} on August {_ord(d1)}."
        if sub == "presupposition":
            s2b = f"Since {leaves} on the {_ord(d2)}, can {name} get the early slot?"
            s2g = f"Since {leaves} on the {_ord(d1)}, can {name} get the early slot?"
            span2 = f"{leaves} on the {_ord(d2)}"
        elif sub == "restatement":
            s2b = f"The departure is on August {_ord(d2)}."
            s2g = f"The departure is on August {_ord(d1)}."
            span2 = f"departure is on August {_ord(d2)}"
        else:
            s2b = f"We're set for the {_ord(d2)}, right - anything left to prepare?"
            s2g = f"We're set for the {_ord(d1)}, right - anything left to prepare?"
            span2 = f"set for the {_ord(d2)}"
        out.append(two_stmt(
            sid, "functional_date", sub, s1, s2b, s2g,
            f"Now draft the {deliverable}.",
            "functional", [f"on August {_ord(d1)}", span2],
            facts=[_fact(sid, "c1", ent, "departure_date", f"2026-08-{d1:02d}",
                         num=20260800 + d1),
                   _fact(sid, "c2", ent, "departure_date", f"2026-08-{d2:02d}",
                         num=20260800 + d2)],
            gap=P(GAPS, i)))
    return out


def f_functional_city(n=102):
    out = []
    for i in range(n):
        sid = f"fcity_{i:03d}"
        c1, c2 = P(CITY_PAIRS, i)
        sub = ["flight", "bus", "freight"][i % 3]
        if sub == "flight":
            s1 = f"We fly out of {c1} for the conference."
            s2b = f"Our flight from {c2} boards at 9am, so let's plan the morning."
            s2g = f"Our flight from {c1} boards at 9am, so let's plan the morning."
            spans = [f"fly out of {c1}", f"flight from {c2}"]
        elif sub == "bus":
            s1 = f"The tour bus departs from {c1}."
            s2b = f"Everyone should be at the {c2} bus terminal by eight."
            s2g = f"Everyone should be at the {c1} bus terminal by eight."
            spans = [f"departs from {c1}", f"the {c2} bus terminal"]
        else:
            s1 = f"The freight leaves from the {c1} depot."
            s2b = f"Loading starts at the {c2} depot at dawn."
            s2g = f"Loading starts at the {c1} depot at dawn."
            spans = [f"leaves from the {c1} depot", f"the {c2} depot"]
        out.append(two_stmt(
            sid, "functional_city", sub, s1, s2b, s2g,
            "Draft the departure-morning plan for the group.",
            "functional", spans,
            facts=[_fact(sid, "c1", "journey", "departure_city", c1.lower()),
                   _fact(sid, "c2", "journey", "departure_city", c2.lower())],
            gap=P(GAPS, i)))
    return out


def f_functional_price(n=102):
    price_pairs = [(2000, 3500), (1500, 900), (4200, 5100), (800, 1400),
                   (2600, 1900), (5000, 6500), (1200, 700), (3300, 2400),
                   (950, 1600), (7800, 9200), (600, 1100), (2900, 2100)]
    ctx = [("trip", "the trip"), ("event", "the launch event"),
           ("project", "the project"), ("renovation", "the renovation"),
           ("campaign", "the campaign")]
    out = []
    for i in range(n):
        sid = f"fprice_{i:03d}"
        p1, p2 = P(price_pairs, i)
        ent, label = P(ctx, i)
        sub = ["direct", "presupposition", "restated_total"][i % 3]
        s1 = f"The total budget for {label} is {p1} USD."
        if sub == "direct":
            s2b = f"The budget is {p2} USD in total."
            s2g = f"The budget is {p1} USD in total."
            span2 = f"budget is {p2} USD"
        elif sub == "presupposition":
            s2b = f"So with our {p2} USD total budget, what should we prioritize?"
            s2g = f"So with our {p1} USD total budget, what should we prioritize?"
            span2 = f"our {p2} USD total budget"
        else:
            s2b = f"Let's keep everything under the {p2} USD total we agreed."
            s2g = f"Let's keep everything under the {p1} USD total we agreed."
            span2 = f"the {p2} USD total"
        out.append(two_stmt(
            sid, "functional_price", sub, s1, s2b, s2g,
            f"Draft the cost plan for {label}.",
            "functional", [f"is {p1} USD", span2],
            facts=[_fact(sid, "c1", ent, "total_price", f"{p1} USD", num=p1),
                   _fact(sid, "c2", ent, "total_price", f"{p2} USD", num=p2)],
            gap=P(GAPS, i)))
    return out


def f_polarity(n=102):
    places = [("venue", "Basel"), ("office", "Zurich"), ("warehouse", "Ghent"),
              ("studio", "Malmo"), ("clinic", "Leiden"), ("lab", "Tartu"),
              ("depot", "Porto"), ("gallery", "Vienna"), ("archive", "Riga"),
              ("workshop", "Oslo"), ("showroom", "Milan"), ("kitchen", "Lisbon")]
    devices = ["elevator", "freight lift", "projector", "espresso machine",
               "badge printer", "AC unit", "backup generator", "intercom",
               "revolving door", "service hatch", "chairlift", "scanner"]
    out = []
    for i in range(n):
        sid = f"pol_{i:03d}"
        sub = ["location", "status", "attendance"][i % 3]
        if sub == "location":
            thing, place = P(places, i)
            s1 = f"The {thing} is in {place}."
            s2b = f"To be clear: the {thing} isn't in {place}."
            s2g = f"To be clear: the {thing} is right in central {place}."
            task = f"Write the directions email for people visiting the {thing}."
            spans = [f"The {thing} is in {place}", f"the {thing} isn't in {place}"]
            facts = [_fact(sid, "c1", thing, "located_in", place.lower()),
                     _fact(sid, "c2", thing, "located_in", place.lower(),
                           modality="negated")]
        elif sub == "status":
            dev = P(devices, i)
            s1 = f"The {dev} is working right now."
            s2b = f"Just so you know, the {dev} is not working right now."
            s2g = f"Just so you know, the {dev} is loud right now but working fine."
            task = f"Write the facilities notice about the {dev}."
            spans = [f"The {dev} is working right now",
                     f"the {dev} is not working right now"]
            ent = dev.replace(" ", "_")
            facts = [_fact(sid, "c1", ent, "status", "working"),
                     _fact(sid, "c2", ent, "status", "working",
                           modality="negated")]
        else:
            name = P(NAMES, i)
            s1 = f"{name} is on the final attendee list."
            s2b = f"By the way, {name} is not on the final attendee list."
            s2g = f"By the way, {name} is on the final attendee list for both days."
            task = "Write the attendee logistics note."
            spans = [f"{name} is on the final attendee list",
                     f"{name} is not on the final attendee list"]
            facts = [_fact(sid, "c1", name.lower(), "attending", "offsite"),
                     _fact(sid, "c2", name.lower(), "attending", "offsite",
                           modality="negated")]
        out.append(two_stmt(sid, "polarity", sub, s1, s2b, s2g, task,
                            "polarity", spans, facts=facts, gap=P(GAPS, i),
                            pool=FILLERS_WORK))
    return out


def f_cycle(n=100):
    triads = [("keynote", "workshop", "demo"), ("rehearsal", "soundcheck", "show"),
              ("screening", "interview", "offer"), ("draft", "review", "signoff"),
              ("prototype", "pilot", "rollout"), ("briefing", "training", "audit"),
              ("warmup", "match", "ceremony"), ("setup", "recording", "mixdown"),
              ("scoping", "build", "handover"), ("tasting", "dinner", "toast"),
              ("checkin", "orientation", "tour"), ("kickoff", "sprint", "retro"),
              ("boarding", "taxi", "takeoff"), ("prep", "service", "cleanup"),
              ("intro", "panel", "networking")]
    phr = [lambda x, y: f"The {x} is before the {y}.",
           lambda x, y: f"The {x} precedes the {y}.",
           lambda x, y: f"The {y} comes after the {x}."]
    out = []
    for i in range(n):
        sid = f"cyc_{i:03d}"
        gap = P(GAPS, i)
        say = P(phr, i)
        sub = "loop3" if i % 2 == 0 else "loop4"
        a, b, c = P(triads, i)
        if sub == "loop3":
            chain = [(a, b), (b, c)]
            closer = f"And the {c} comes before the {a}, right after lunch."
            clean = f"And the {c} wraps up the day, right after lunch."
            events = (a, b, c)
        else:
            d = "afterparty" if c != "ceremony" else "banquet"
            chain = [(a, b), (b, c), (c, d)]
            closer = f"And the {d} comes before the {a}, to kick things off."
            clean = f"And the {d} closes the whole day out."
            events = (a, b, c, d)
        opening = []
        chain_sentences = []
        for x, y in chain:
            sent = say(x, y)
            chain_sentences.append(sent)
            opening.append(("user", sent))
            opening.append(("assistant", "Noted."))
        pairs = _weave(opening, [("user", closer)], gap, pool=FILLERS_WORK)
        spans = [s.rstrip(".") for s in chain_sentences] + [closer.rstrip(".")]
        out.append({
            "id": sid, "family": "cycle", "sub": sub, "control": False,
            "gap_turns": gap * 2, "messages": _msgs(pairs),
            "clean_fix": {len(pairs) - 1: clean},
            "task": "Draft the event schedule as a timeline.",
            "conflict": {"kind": "cycle", "spans": spans},
            "facts": [], "constraints": [], "same_as": [],
            "before": [(f"{sid}__b{j}", f"{sid}__{x}", f"{sid}__{y}")
                       for j, (x, y) in enumerate(chain)] +
                      [(f"{sid}__bz", f"{sid}__{events[-1]}", f"{sid}__{events[0]}")],
            "expect_kinds": ["cycle"],
        })
    return out


def f_relation(n=102):
    depts = ["Design", "Platform", "Research", "Support", "Growth", "Data",
             "Ops", "Legal", "Finance", "Security"]
    out = []
    for i in range(n):
        sid = f"rel_{i:03d}"
        sub = ["mutual_pair", "self_relation", "hierarchy_loop"][i % 3]
        n1, n2, n3 = P(NAMES, 2 * i), P(NAMES, 2 * i + 7), P(NAMES, 2 * i + 13)
        if sub == "mutual_pair":
            attr = ["parent_of", "older_than", "manager_of"][(i // 3) % 3]
            word = {"parent_of": ("is {}'s parent", "Fill in the family tree entry."),
                    "older_than": ("is older than {}", "Write the seniority note."),
                    "manager_of": ("manages {}", "Write the reporting note.")}[attr]
            s = (f"{n1} {word[0].format(n2)}. "
                 f"{n2} {word[0].format(n1)}.")
            good = (f"{n1} {word[0].format(n2)}. "
                    f"{n2} {word[0].format(n3)}.")
            kinds = {"parent_of": ["asymmetry", "cycle"],
                     "older_than": ["asymmetry"],
                     "manager_of": ["asymmetry"]}[attr]
            facts = [_fact(sid, "c1", n1.lower(), attr, f"{sid}__{n2.lower()}"),
                     _fact(sid, "c2", n2.lower(), attr, f"{sid}__{n1.lower()}")]
            spans = [f"{n1} {word[0].format(n2)}", f"{n2} {word[0].format(n1)}"]
            out.append(one_stmt(sid, "relation", sub, s, good, word[1],
                                "/".join(kinds), spans, facts=facts,
                                expect=kinds))
        elif sub == "self_relation":
            attr, phrase = [("manager_of", "manages"), ("married_to", "is married to"),
                            ("sibling_of", "is a sibling of")][(i // 3) % 3]
            ref = "herself" if i % 2 else "himself"
            s = f"{n1} {phrase} {ref}, officially."
            good = f"{n1} {phrase} {n2}, officially."
            facts = [_fact(sid, "c1", n1.lower(), attr, f"{sid}__{n1.lower()}")]
            out.append(one_stmt(sid, "relation", sub, s, good,
                                f"Write {n1}'s profile line for the org page.",
                                "irreflexive", [s.rstrip(".")], facts=facts,
                                expect=["irreflexive"]))
        else:
            attr, verb = [("reports_to", "reports to"),
                          ("part_of", "is part of"),
                          ("ancestor_of", "is an ancestor of")][(i // 3) % 3]
            if attr == "part_of":
                a, b, c = P(depts, i), P(depts, i + 3), P(depts, i + 6)
            else:
                a, b, c = n1, n2, n3
            s = (f"{a} {verb} {b}, {b} {verb} {c}, "
                 f"and {c} {verb} {a}.")
            good = (f"{a} {verb} {b}, {b} {verb} {c}, "
                    f"and {c} {verb} the parent group.")
            facts = [_fact(sid, "c1", a.lower(), attr, f"{sid}__{b.lower()}"),
                     _fact(sid, "c2", b.lower(), attr, f"{sid}__{c.lower()}"),
                     _fact(sid, "c3", c.lower(), attr, f"{sid}__{a.lower()}")]
            out.append(one_stmt(sid, "relation", sub, s, good,
                                "Write the structure section for the handbook.",
                                "cycle", [f"{c} {verb} {a}"], facts=facts,
                                expect=["cycle"]))
    return out


def f_interval(n=102):
    out = []
    for i in range(n):
        sid = f"intv_{i:03d}"
        gap = P(GAPS, i)
        d1, d2 = P(DATE_PAIRS, i)
        name = P(NAMES, i)
        sub = ["trip", "lifespan", "engagement"][i % 3]
        if sub == "trip":
            s1 = f"We depart on August {_ord(d1)}."
            s2b, s2g = (f"We're back on August {_ord(d2)}.",
                        f"We're back on August {_ord(min(d1 + 9, 30))}.")
            task = "Draft the booking summary for the travel agent."
            facts = [_fact(sid, "c1", "trip", "departure_date",
                           f"2026-08-{d1:02d}", num=20260800 + d1),
                     _fact(sid, "c2", "trip", "return_date",
                           f"2026-08-{d2:02d}", num=20260800 + d2)]
        elif sub == "lifespan":
            y1, y2 = 1900 + (i * 3) % 90, 1900 + (i * 3) % 90 - 4 - (i % 6)
            s1 = f"{name} was born in {y1}."
            s2b = f"{name} died in {y2}."
            s2g = f"{name} died in {min(y1 + 41, 2024)}."
            task = "Write the short biography paragraph."
            facts = [_fact(sid, "c1", name.lower(), "birth_date", str(y1),
                           num=y1 * 10000 + 101),
                     _fact(sid, "c2", name.lower(), "death_date", str(y2),
                           num=y2 * 10000 + 101)]
        else:
            kind2 = [("hotel_stay", "check_in", "check_out",
                      "Check-in is on August {a}.",
                      "Check-out is on August {b}.",
                      "Confirm the hotel reservation details."),
                     ("project", "start_date", "end_date",
                      "The project starts on August {a}.",
                      "The project ends on August {b}.",
                      "Write the project timeline summary."),
                     ("session", "start_time", "end_time",
                      "The session starts at {a}:00.",
                      "It ends at {b}:00 the same day.",
                      "Write the session logistics note.")][(i // 3) % 3]
            ent, a_attr, b_attr, t1, t2, task = kind2
            if a_attr == "start_time":
                a, b = 13 + (i % 6), 8 + (i % 4)
                facts = [_fact(sid, "c1", ent, a_attr, f"{a}:00", num=a * 100),
                         _fact(sid, "c2", ent, b_attr, f"{b}:00", num=b * 100)]
                s2g = t2.format(b=a + 2)
                s1, s2b = t1.format(a=a), t2.format(b=b)
            else:
                a, b = d1, d2
                facts = [_fact(sid, "c1", ent, a_attr, f"2026-08-{a:02d}",
                               num=20260800 + a),
                         _fact(sid, "c2", ent, b_attr, f"2026-08-{b:02d}",
                               num=20260800 + b)]
                s2g = t2.format(b=_ord(min(a + 5, 30)))
                s1, s2b = t1.format(a=_ord(a)), t2.format(b=_ord(b))
        out.append(two_stmt(sid, "interval", sub, s1, s2b, s2g, task,
                            "interval_order",
                            [s1.rstrip("."), s2b.rstrip(".")],
                            facts=facts, gap=gap,
                            expect=["interval_order"]))
    return out


def f_range(n=102):
    people = ["Grandpa", "Grandma", "Uncle Theo", "our neighbor",
              "the librarian", "Aunt Vera", "the caretaker", "the coach",
              "our landlord", "the janitor", "the gardener", "cousin Mila"]
    procs = ["rollout", "migration", "test coverage", "adoption",
             "the survey", "onboarding", "the backup", "indexing",
             "the sync", "training", "the audit", "localization"]
    rooms = ["main hall", "auditorium", "studio B", "conference room",
             "rooftop deck", "atrium", "lecture hall", "gallery",
             "banquet room", "annex", "courtyard", "wine cellar"]
    out = []
    for i in range(n):
        sid = f"rng_{i:03d}"
        sub = ["age_over", "percent_over", "negative_capacity"][i % 3]
        if sub == "age_over":
            subj = P(people, i)
            age = 135 + 6 * (i % 30)
            bad = f"{subj} turns {age} this year."
            good = f"{subj} turns {58 + (i % 32)} this year."
            task = "Write the birthday invitation text."
            ent = subj.lower().replace(" ", "_")
            facts = [_fact(sid, "c1", ent, "age", str(age), num=age)]
        elif sub == "percent_over":
            subj = P(procs, i)
            pct = 104 + 9 * (i % 30)
            bad = f"The {subj} is {pct}% complete."
            good = f"The {subj} is {48 + (i % 47)}% complete."
            task = "Write the status update for stakeholders."
            ent = subj.replace("the ", "").replace(" ", "_")
            facts = [_fact(sid, "c1", ent, "percentage", f"{pct} percent",
                           num=pct)]
        else:
            room = P(rooms, i)
            cap = -(8 + 4 * (i % 30))
            bad = f"The {room} has a capacity of {cap} seats."
            good = f"The {room} has a capacity of {70 + 6 * (i % 12)} seats."
            task = "Write the venue briefing for the organizers."
            ent = room.replace(" ", "_")
            facts = [_fact(sid, "c1", ent, "capacity", str(cap), num=cap)]
        out.append(one_stmt(sid, "range", sub, bad, good, task, "range",
                            [bad.rstrip(".")], facts=facts, expect=["range"]))
    return out


def _andlist(ms):
    return (f"{ms[0]} and {ms[1]}" if len(ms) == 2
            else ", ".join(ms[:-1]) + f", and {ms[-1]}")


def f_cardinality(n=102):
    groups = ["team", "panel", "committee", "band", "crew", "jury", "squad",
              "cohort", "delegation", "taskforce", "chapter", "ensemble"]
    out = []
    for i in range(n):
        sid = f"card_{i:03d}"
        group = P(groups, i)
        cnt = 2 + (i % 3)
        members = [P(NAMES, 3 * i + j) for j in range(cnt + 1)]
        listed = _andlist(members)
        sub = ["inline", "split", "booking"][i % 3]
        facts = [_fact(sid, "c0", group, "member_count", str(cnt), num=cnt)]
        facts += [_fact(sid, f"m{j}", group, "has_member",
                        f"{sid}__{m.lower()}") for j, m in enumerate(members)]
        if sub == "inline":
            bad = f"The {group} is {cnt} people: {listed}."
            good = f"The {group} is {cnt + 1} people: {listed}."
            out.append(one_stmt(sid, "cardinality", sub, bad, good,
                                f"Write the {group} intro paragraph.",
                                "cardinality", [bad.rstrip(".")],
                                facts=facts, expect=["cardinality"]))
        elif sub == "split":
            s1 = f"The {group} is {cnt} people in total."
            s2b = f"{listed} will each present five minutes."
            s2g = _andlist(members[:cnt]) + " will each present five minutes."
            sc = two_stmt(sid, "cardinality", sub, s1, s2b, s2g,
                          f"Write the {group} intro paragraph.",
                          "cardinality",
                          [f"{group} is {cnt} people", listed],
                          facts=facts, gap=P(GAPS, i),
                          expect=["cardinality"], pool=FILLERS_WORK)
            out.append(sc)
        else:
            bad = (f"We're a party of {cnt}: {listed} are all coming.")
            good = (f"We're a party of {cnt + 1}: {listed} are all coming.")
            out.append(one_stmt(sid, "cardinality", sub, bad, good,
                                "Write the dinner confirmation message.",
                                "cardinality", [bad.rstrip(".")],
                                facts=facts, expect=["cardinality"]))
    return out


def f_instruction(n=102):
    topics = [("pricing", "include the final price table"),
              ("competitor names", "compare us with at least two competitors"),
              ("discounts", "mention the current discount"),
              ("refunds", "explain the refund policy"),
              ("internal metrics", "quote our internal metrics"),
              ("personal opinions", "give your personal recommendation"),
              ("legal advice", "state the legal implications"),
              ("emojis", "use at least three emojis"),
              ("links", "include links to sources"),
              ("apologies", "open with an apology"),
              ("humor", "include a light joke"),
              ("estimates", "provide a numeric estimate")]
    personas = [("Aria", "Max"), ("Nova", "Juno"), ("Sage", "Rex"),
                ("Echo", "Vera"), ("Iris", "Otto"), ("Luna", "Bram"),
                ("Faye", "Nils"), ("Wren", "Kito"), ("Isla", "Dov"),
                ("Mireille", "Tobias"), ("Anouk", "Casper"), ("Sanne", "Jorn")]
    windows = [(300, 500), (50, 400), (100, 250), (200, 600), (80, 150),
               (150, 900), (40, 120), (250, 700), (60, 300), (90, 220),
               (30, 180), (120, 480)]
    tasks = ["Plan a weekend in Kyoto for me.",
             "Help me choose a laptop for travel.",
             "Summarize the main sights of Barcelona.",
             "Explain how solar panels work.",
             "Draft a welcome message for new customers.",
             "What should we do on our first evening in Rome?",
             "Outline a beginner running plan.",
             "Compare train vs plane for Paris to Amsterdam."]
    # persona clashes must BIND: the task has to force self-identification,
    # otherwise "silently obeys one side" is unmeasurable
    persona_tasks = ["Introduce yourself, then help me plan a quick trip.",
                     "Draft the welcome email and sign it with your name.",
                     "Tell me who I'm chatting with, then summarize what you can do."]
    out = []
    for i in range(n):
        sid = f"sys_{i:03d}"
        sub = ["forbid_require", "dual_persona", "impossible_window"][i % 3]
        fillers = [0, 3, 8][(i // 3) % 3]
        pad = FILLER_INSTRUCTIONS[:fillers]
        task = P(persona_tasks, i // 3) if sub == "dual_persona" else P(tasks, i)
        if sub == "forbid_require":
            topic, req = P(topics, i // 3)
            first, second = f"Never mention {topic}.", f"Always {req}."
            good = f"Always {req}."
            tkey = topic.replace(" ", "_")
            constraints = [("k1", "forbid", tkey, ""), ("k2", "require", tkey, "")]
            spans = [first.rstrip("."), second.rstrip(".")]
        elif sub == "dual_persona":
            p1, p2 = P(personas, i // 3)
            first, second = (f"You are {p1}, a formal assistant.",
                             f"Your name is {p2}.")
            good = f"You are {p1}, a formal assistant."
            constraints = [("k1", "persona", "assistant_identity", p1.lower()),
                           ("k2", "persona", "assistant_identity", p2.lower())]
            spans = [f"You are {p1}", f"Your name is {p2}"]
        else:
            hi, lo = P(windows, i // 3)
            first, second = (f"Reply in at most {hi} words.",
                             f"Never reply in under {lo} words.")
            good = f"Reply in roughly {hi} to {lo} words."
            constraints = [("k1", "max_value", "word_count", hi),
                           ("k2", "min_value", "word_count", lo)]
            spans = [f"at most {hi} words", f"under {lo} words"]
        system_bad = " ".join([first] + pad + [second])
        system_good = " ".join(pad + [good]) if pad else good
        out.append({
            "id": sid, "family": "instruction_clash", "sub": sub,
            "control": False, "gap_turns": 0,
            "messages": _msgs([("system", system_bad)]),
            "clean_fix": {0: system_good}, "task": task,
            "conflict": {"kind": "instruction_clash", "spans": spans},
            "facts": [], "before": [], "same_as": [],
            "constraints": constraints,
            "expect_kinds": ["instruction_clash"],
            "span_distance_lines": fillers,
        })
    return out


def f_spatial(n=102):
    rooms = ["archive", "annex", "west wing", "studio", "print room",
             "cold storage", "server room", "mezzanine", "workshop", "vault",
             "reading room", "loading bay"]
    companies = ["Nortide", "Velora", "Kastel", "Brimford", "Ostrale",
                 "Quenta", "Marlow Labs", "Deverin", "Toska", "Ferrant",
                 "Halvik", "Ostrom"]
    sites = [("depot", "mill"), ("pier", "lighthouse"), ("barn", "silo"),
             ("gatehouse", "stables"), ("kiln", "cooperage"),
             ("boathouse", "chapel"), ("tannery", "granary"),
             ("forge", "brewhouse"), ("windmill", "icehouse"),
             ("smokehouse", "orchard shed"), ("tollhouse", "ferry dock"),
             ("waterworks", "pumphouse")]
    dirs = ["north", "south", "east", "west"]
    out = []
    for i in range(n):
        sid = f"spat_{i:03d}"
        sub = ["containment_loop", "two_headquarters", "direction_reversal"][i % 3]
        gap = P(GAPS, i)
        if sub == "containment_loop":
            r1, r2, r3 = P(rooms, i), P(rooms, i + 4), P(rooms, i + 8)
            opening = [("user", f"The {r1} is inside the {r2}."),
                       ("assistant", "Noted."),
                       ("user", f"The {r2} is inside the {r3}."),
                       ("assistant", "Got it.")]
            closer = f"And the {r3} is inside the {r1}."
            clean = f"And the {r3} is inside the main building."
            pairs = _weave(opening, [("user", closer)], gap, pool=FILLERS_WORK)
            out.append({
                "id": sid, "family": "spatial", "sub": sub, "control": False,
                "gap_turns": gap * 2, "messages": _msgs(pairs),
                "clean_fix": {len(pairs) - 1: clean},
                "task": "Write the site access guide describing how to reach each room.",
                "conflict": {"kind": "cycle",
                             "spans": [f"{r1} is inside the {r2}",
                                       f"{r2} is inside the {r3}",
                                       f"{r3} is inside the {r1}"]},
                "facts": [_fact(sid, "c1", r1.replace(" ", "_"), "located_in",
                                f"{sid}__{r2.replace(' ', '_')}"),
                          _fact(sid, "c2", r2.replace(" ", "_"), "located_in",
                                f"{sid}__{r3.replace(' ', '_')}"),
                          _fact(sid, "c3", r3.replace(" ", "_"), "located_in",
                                f"{sid}__{r1.replace(' ', '_')}")],
                "before": [], "constraints": [], "same_as": [],
                "expect_kinds": ["cycle"],
            })
        elif sub == "two_headquarters":
            co = P(companies, i)
            c1, c2 = P(CITIES, i), P(CITIES, i + 7)
            s1 = f"{co} is headquartered in {c1}."
            s2b = f"The team will visit {co} at its {c2} headquarters next month."
            s2g = f"The team will visit {co} at its {c1} headquarters next month."
            out.append(two_stmt(
                sid, "spatial", sub, s1, s2b, s2g,
                f"Write the company fact sheet for {co}.",
                "functional",
                [f"headquartered in {c1}", f"its {c2} headquarters"],
                facts=[_fact(sid, "c1", co.lower().replace(" ", "_"),
                             "headquartered_in", c1.lower()),
                       _fact(sid, "c2", co.lower().replace(" ", "_"),
                             "headquartered_in", c2.lower())],
                gap=gap, expect=["functional"], pool=FILLERS_WORK))
        else:
            s1n, s2n = P(sites, i)
            d = P(dirs, i)
            s1 = f"The {s1n} is {d} of the {s2n}."
            s2b = f"And the {s2n} is {d} of the {s1n}, past the fence."
            opp = {"north": "south", "south": "north",
                   "east": "west", "west": "east"}[d]
            s2g = f"And the {s2n} is {opp} of the {s1n}, past the fence."
            e1, e2 = s1n.replace(" ", "_"), s2n.replace(" ", "_")
            sc = two_stmt(
                sid, "spatial", sub, s1, s2b, s2g,
                "Write the walking directions between the two buildings.",
                "asymmetry",
                [f"{s1n} is {d} of the {s2n}", f"{s2n} is {d} of the {s1n}"],
                facts=[_fact(sid, "c1", e1, f"{d}_of", f"{sid}__{e2}"),
                       _fact(sid, "c2", e2, f"{d}_of", f"{sid}__{e1}")],
                gap=gap, expect=["asymmetry"], pool=FILLERS_WORK)
            sc["ontology"] = [("asymmetric", f"{d}_of")]
            out.append(sc)
    return out


def f_causal(n=102):
    pairs2 = [("outage", "failed deploy"), ("delay", "port strike"),
              ("recall", "sensor defect"), ("flood", "burst main"),
              ("blackout", "grid fault"), ("data loss", "disk crash"),
              ("fire", "short circuit"), ("shortage", "panic buying"),
              ("crash", "memory leak"), ("breach", "phished password"),
              ("spill", "valve fault"), ("stall", "fuel clog")]
    triads = [("budget cut", "delay", "overtime spike"),
              ("churn", "price hike", "revenue dip"),
              ("bottleneck", "rework", "missed deadline"),
              ("overheating", "throttling", "slow test run"),
              ("attrition", "overload", "burnout wave"),
              ("misroute", "backlog", "expedite fee"),
              ("stockout", "rush order", "quality slip"),
              ("regression", "rollback", "release freeze"),
              ("alert storm", "fatigue", "missed page"),
              ("scope creep", "crunch", "defect spike"),
              ("late invoice", "cash gap", "hiring freeze"),
              ("packet loss", "retries", "queue buildup")]
    orders = [("fire", "alarm"), ("leak", "shutdown"),
              ("bug report", "hotfix"), ("storm", "cancellation"),
              ("injury", "substitution"), ("complaint", "refund"),
              ("frost", "crop loss"), ("recall notice", "return wave"),
              ("power dip", "reboot"), ("audit finding", "policy change"),
              ("gust", "scaffold halt"), ("false alarm", "evacuation")]
    out = []
    for i in range(n):
        sid = f"caus_{i:03d}"
        sub = ["loop2", "loop3", "effect_before_cause"][i % 3]
        gap = P(GAPS, i)
        if sub == "loop2":
            eff, cse = P(pairs2, i)
            self_cause = (i // 3) % 5 == 4
            if self_cause:
                s1 = f"The {eff} was caused by the {cse}."
                s2b = f"Digging deeper, the {eff} ultimately caused itself."
                s2g = f"Digging deeper, the {eff} traces back to a config typo."
                e1 = eff.replace(" ", "_")
                facts = [_fact(sid, "c1", e1, "caused_by", f"{sid}__{e1}")]
                spans = [f"the {eff} ultimately caused itself"]
            else:
                s1 = f"The {eff} was caused by the {cse}."
                s2b = f"And the {cse} itself was caused by the {eff}."
                s2g = f"And the {cse} itself was caused by aging hardware."
                e1, e2 = eff.replace(" ", "_"), cse.replace(" ", "_")
                facts = [_fact(sid, "c1", e1, "caused_by", f"{sid}__{e2}"),
                         _fact(sid, "c2", e2, "caused_by", f"{sid}__{e1}")]
                spans = [f"{eff} was caused by the {cse}",
                         f"{cse} itself was caused by the {eff}"]
            out.append(two_stmt(sid, "causal", sub, s1, s2b, s2g,
                                "Write the incident postmortem summary.",
                                "cycle", spans, facts=facts, gap=gap,
                                expect=["cycle"], pool=FILLERS_WORK))
        elif sub == "loop3":
            a, b, c = P(triads, i)
            opening = [("user", f"The {a} caused the {b}."),
                       ("assistant", "Noted."),
                       ("user", f"The {b} caused the {c}."),
                       ("assistant", "Understood.")]
            closer = f"And the {c} caused the {a} in the first place."
            clean = f"And the {c} caused a hiring push."
            pairs = _weave(opening, [("user", closer)], gap, pool=FILLERS_WORK)
            ea, eb, ec = (x.replace(" ", "_") for x in (a, b, c))
            out.append({
                "id": sid, "family": "causal", "sub": sub, "control": False,
                "gap_turns": gap * 2, "messages": _msgs(pairs),
                "clean_fix": {len(pairs) - 1: clean},
                "task": "Write the retrospective paragraph explaining what led to what.",
                "conflict": {"kind": "cycle",
                             "spans": [f"{a} caused the {b}",
                                       f"{b} caused the {c}",
                                       f"{c} caused the {a}"]},
                "facts": [_fact(sid, "c1", eb, "caused_by", f"{sid}__{ea}"),
                          _fact(sid, "c2", ec, "caused_by", f"{sid}__{eb}"),
                          _fact(sid, "c3", ea, "caused_by", f"{sid}__{ec}")],
                "before": [], "constraints": [], "same_as": [],
                "expect_kinds": ["cycle"],
            })
        else:
            cse, eff = P(orders, i)
            s1 = f"The {eff} was triggered by the {cse}."
            s2b = f"The {eff} happened well before the {cse}, according to the log."
            s2g = f"The {eff} happened right after the {cse}, according to the log."
            e1, e2 = cse.replace(" ", "_"), eff.replace(" ", "_")
            sc = two_stmt(sid, "causal", sub, s1, s2b, s2g,
                          "Draft the incident timeline.",
                          "cycle",
                          [f"{eff} was triggered by the {cse}",
                           f"{eff} happened well before the {cse}"],
                          gap=gap, expect=["cycle"], pool=FILLERS_WORK)
            sc["before"] = [(f"{sid}__b1", f"{sid}__{e1}", f"{sid}__{e2}"),
                            (f"{sid}__b2", f"{sid}__{e2}", f"{sid}__{e1}")]
            out.append(sc)
    return out


def f_disjoint(n=102):
    orgs = ["Acme", "Bolt & Co", "Ferrant", "Halvik", "Nortide", "Ostrale",
            "Quenta", "Toska", "Velora", "Kastel", "Brimford", "Deverin"]
    events = ["the jubilee", "the harvest fair", "the summit", "the regatta",
              "the vernissage", "the hackathon", "the gala", "the derby",
              "the symposium", "the auction", "the premiere", "the parade"]
    out = []
    for i in range(n):
        sid = f"disj_{i:03d}"
        sub = ["org_location", "person_org", "person_event"][i % 3]
        gap = P(GAPS, i)
        if sub == "org_location":
            co = P(orgs, i)
            s1 = f"{co} is our supplier - great company to work with."
            s2b = f"The reception is at {co}, the conference venue downtown."
            s2g = "The reception is at the Kongresshalle, the conference venue downtown."
            ent = co.lower().replace(" & ", "_").replace(" ", "_")
            facts = [_fact(sid, "c1", ent, "is_a", "organization"),
                     _fact(sid, "c2", ent, "is_a", "location")]
            spans = [f"{co} is our supplier", f"at {co}, the conference venue"]
            task = f"Draft the vendor briefing note about {co}."
        elif sub == "person_org":
            nm = P(NAMES, i)
            s1 = f"{nm} joined our advisory board last spring."
            s2b = f"We're negotiating the contract with {nm}, the consultancy firm."
            s2g = "We're negotiating the contract with Bolt & Co, the consultancy firm."
            facts = [_fact(sid, "c1", nm.lower(), "is_a", "person"),
                     _fact(sid, "c2", nm.lower(), "is_a", "organization")]
            spans = [f"{nm} joined our advisory board",
                     f"{nm}, the consultancy firm"]
            task = "Write the stakeholder overview paragraph."
        else:
            nm = P(NAMES, i + 11)
            ev = P(events, i)
            s1 = f"{nm} will open {ev} with a short speech."
            s2b = f"By the way, {nm} takes place on Friday evening."
            s2g = f"By the way, {ev} takes place on Friday evening."
            facts = [_fact(sid, "c1", nm.lower(), "is_a", "person"),
                     _fact(sid, "c2", nm.lower(), "is_a", "event")]
            spans = [f"{nm} will open {ev}", f"{nm} takes place on Friday"]
            task = "Write the program note for the opening."
        out.append(two_stmt(sid, "disjoint_class", sub, s1, s2b, s2g, task,
                            "disjoint_class", spans, facts=facts, gap=gap,
                            expect=["disjoint_class"], pool=FILLERS_WORK))
    return out


def f_domain(n=102):
    events = ["kickoff meeting", "quarterly review", "fire drill",
              "board session", "town hall", "standup", "retrospective",
              "signing ceremony", "onboarding day", "audit week",
              "press briefing", "planning offsite"]
    orgs = ["the committee", "the vendor", "the consultancy", "the guild",
            "the cooperative", "the syndicate", "the trust", "the bureau",
            "the collective", "the chamber", "the union", "the agency"]
    bloods = ["O", "A", "B", "AB"]
    out = []
    for i in range(n):
        sid = f"dom_{i:03d}"
        sub = ["blood_type_event", "passport_org", "marital_event"][i % 3]
        if sub == "blood_type_event":
            ev = P(events, i)
            bt = P(bloods, i)
            bad = f"For the records: the {ev}'s blood type is {bt}."
            good = f"For the records: the {ev}'s room number is {200 + i % 40}."
            ent = ev.replace(" ", "_")
            facts = [_fact(sid, "c1", ent, "is_a", "event"),
                     _fact(sid, "c2", ent, "blood_type", bt.lower())]
            task = "Write up the meeting record."
            spans = [f"the {ev}'s blood type is {bt}"]
        elif sub == "passport_org":
            org = P(orgs, i)
            num = f"K{2400 + i * 7}"
            bad = f"Please note {org}'s passport number, {num}, for the file."
            good = f"Please note {org}'s registration number, R-{2400 + i * 7}, for the file."
            ent = org.replace("the ", "").replace(" ", "_")
            facts = [_fact(sid, "c1", ent, "is_a", "organization"),
                     _fact(sid, "c2", ent, "passport_number", num)]
            task = "Prepare the compliance file summary."
            spans = [f"{org}'s passport number, {num}"]
        else:
            ev = P(events, i + 5)
            status = ["married", "single", "divorced"][i % 3]
            bad = f"HR asked us to log the {ev}'s marital status as {status}."
            good = f"HR asked us to log the {ev}'s attendance count as {40 + i % 30}."
            ent = ev.replace(" ", "_")
            facts = [_fact(sid, "c1", ent, "is_a", "event"),
                     _fact(sid, "c2", ent, "marital_status", status)]
            task = "Write the HR log entry."
            spans = [f"the {ev}'s marital status as {status}"]
        out.append(one_stmt(sid, "domain_violation", sub, bad, good, task,
                            "domain", spans, facts=facts, expect=["domain"]))
    return out


def f_identity(n=100):
    # Only identifiers that are near-universally one-per-person: emails,
    # order numbers, and booking references are shareable in the real world
    # and are excluded from the corpus AND the pack seeds (see DATASET.md,
    # Design principles).
    ids = [("passport_number", "passport", "K{v}"),
           ("ssn", "social security number", "SSN-{v}")]
    out = []
    for i in range(n):
        sid = f"ident_{i:03d}"
        sub = "shared_identifier" if i % 2 == 0 else "same_person_clash"
        gap = P(GAPS, i)
        n1, n2 = P(NAMES, 2 * i), P(NAMES, 2 * i + 9)
        if sub == "shared_identifier":
            attr, label, fmt = P(ids, i // 2)
            val = fmt.format(v=4100 + i * 13)
            s1 = f"{n1}'s {label} is {val}."
            s2b = (f"{n2}'s {label} is {val} as well - and no, they are "
                   f"definitely two different people.")
            s2g = (f"{n2}'s {label} is {fmt.format(v=8200 + i * 13)} - and no, "
                   f"they are definitely two different people.")
            facts = [_fact(sid, "c1", n1.lower(), attr, val),
                     _fact(sid, "c2", n2.lower(), attr, val),
                     _fact(sid, "c3", n1.lower(), "distinct_from",
                           f"{sid}__{n2.lower()}")]
            sc = two_stmt(sid, "identity", sub, s1, s2b, s2g,
                          "Fill in the traveler details for both bookings.",
                          "identity",
                          [f"{n1}'s {label} is {val}",
                           f"{n2}'s {label} is {val}"],
                          facts=facts, gap=gap, expect=["identity"],
                          pool=FILLERS_WORK)
        else:
            alias = f"{n2[0]}. {P(['Costa','Berg','Lindt','Marek','Sousa','Vik'], i)}"
            a1, a2 = 28 + (i % 30), 35 + (i % 30)
            s1 = f"{n1} and {alias} are the same person, just so you know."
            s2b = f"{n1} is {a1}, and {alias} is {a2}."
            s2g = f"{n1} is {a1}, and so of course {alias} is {a1} too."
            ent2 = alias.lower().replace(". ", "_").replace(" ", "_")
            facts = [_fact(sid, "c1", n1.lower(), "age", str(a1), num=a1),
                     _fact(sid, "c2", ent2, "age", str(a2), num=a2)]
            sc = two_stmt(sid, "identity", sub, s1, s2b, s2g,
                          "Write the guest profile for the badge system.",
                          "functional",
                          [f"{n1} and {alias} are the same person",
                           f"{n1} is {a1}, and {alias} is {a2}"],
                          facts=facts, gap=gap, expect=["functional"],
                          pool=FILLERS_WORK)
            sc["same_as"] = [(f"{sid}__{n1.lower()}", f"{sid}__{ent2}")]
        out.append(sc)
    return out


def f_controls(n=102):
    markers = ["Actually, scratch that -", "Correction:", "Update:", "No wait -",
               "I misspoke -", "Make that", "Let me fix that -", "Scrap that -",
               "On second thought -", "Small correction:"]
    out = []
    for i in range(n):
        sid = f"ctrl_{i:03d}"
        sub = ["correction", "restatement", "modality"][i % 3]
        d1, d2 = P(DATE_PAIRS, i)
        if sub == "correction":
            m = P(markers, i)
            k = (i // 3) % 3
            if k == 0:
                s1, s2 = (f"We leave on August {_ord(d2)}.",
                          f"{m} we leave on August {_ord(d1)}.")
                task = "Draft the booking summary for the travel agent."
            elif k == 1:
                c1, c2 = P(CITIES, i), P(CITIES, i + 4)
                s1, s2 = f"The venue is in {c1}.", f"{m} the venue is in {c2}."
                task = "Write the directions email for attendees."
            else:
                p1, p2 = 1000 + 250 * (i % 8), 1500 + 300 * (i % 8)
                s1, s2 = f"The budget is {p1} USD.", f"{m} the budget is {p2} USD."
                task = "Draft the trip cost plan."
        elif sub == "restatement":
            k = (i // 3) % 3
            if k == 0:
                p = 1000 + 500 * (i % 7)
                s1, s2 = (f"Budget's ${p // 1000}k.",
                          f"So with the {p} USD budget, what fits?")
                task = "Draft the trip cost plan."
            elif k == 1:
                s1, s2 = (f"We leave on August {_ord(d1)}.",
                          f"Departure on the {_ord(d1)} - anything left to prep?")
                task = "Draft the booking summary for the travel agent."
            else:
                c = P(CITIES, i)
                s1, s2 = (f"The venue is in {c}.",
                          f"Since the venue's in {c}, let's book nearby hotels.")
                task = "Write the directions email for attendees."
        else:
            k = (i // 3) % 3
            if k == 0:
                c1, c2 = P(CITY_PAIRS, i)
                s1, s2 = (f"We fly out of {c1}.",
                          f"It might be {c2} we leave from, let me confirm.")
                task = "Draft the departure-morning plan for the group."
            elif k == 1:
                s1, s2 = (f"We leave on August {_ord(d1)}.",
                          f"Would it be crazy to leave on the {_ord(d2)} instead?")
                task = "Draft the booking summary for the travel agent."
            else:
                s1, s2 = (f"The demo is before the keynote.",
                          "If the room isn't ready, the keynote goes first instead.")
                task = "Draft the event schedule as a timeline."
        pairs = [("user", s1), ("assistant", "Noted."), ("user", s2)]
        out.append({
            "id": sid, "family": "correction_control", "sub": sub,
            "control": True, "gap_turns": 0, "messages": _msgs(pairs),
            "clean_fix": {}, "task": task, "conflict": None,
            "facts": [], "before": [], "constraints": [], "same_as": [],
            "expect_kinds": [],
        })
    return out


# ---------------------------------------------------------------------------
# Labels
# ---------------------------------------------------------------------------

CATEGORY_VOCABULARY = [
    "value", "negation", "temporal", "spatial", "causal", "structural",
    "numeric", "counting", "identity", "classification", "instruction",
    "output", "control", "gap",
]


def derive_categories(s):
    fam, sub = s["family"], s.get("sub", "")
    fixed = {
        "functional_date": ["value", "temporal"],
        "functional_city": ["value", "spatial"],
        "functional_price": ["value", "numeric"],
        "cycle": ["temporal"],
        "interval": ["temporal"],
        "range": ["numeric"],
        "cardinality": ["counting"],
        "correction_control": ["control"],
        "disjoint_class": ["classification"],
        "domain_violation": ["classification"],
    }
    if fam in fixed:
        return fixed[fam]
    if fam == "polarity":
        return ["negation", "spatial"] if sub == "location" else ["negation"]
    if fam == "relation":
        attrs = {f["attribute"] for f in s.get("facts", [])}
        cats = ["structural"]
        if "older_than" in attrs:
            cats.append("temporal")
        return cats
    if fam == "instruction_clash":
        return (["instruction", "numeric"] if sub == "impossible_window"
                else ["instruction"])
    if fam == "spatial":
        return (["spatial", "value"] if sub == "two_headquarters"
                else ["spatial", "structural"])
    if fam == "causal":
        return (["causal", "temporal"] if sub == "effect_before_cause"
                else ["causal", "structural"])
    if fam == "identity":
        return (["identity"] if sub == "shared_identifier"
                else ["identity", "value"])
    raise ValueError(f"unlabeled family: {fam}")


def attach_labels(rows):
    fam_index = {}
    for s in rows:
        cats = derive_categories(s)
        assert cats and all(c in CATEGORY_VOCABULARY for c in cats), s["id"]
        idx = fam_index.get(s["family"], 0)
        fam_index[s["family"]] = idx + 1
        tier = "smoke" if idx < 10 else ("standard" if idx < 34 else "full")
        distant = (s["gap_turns"] >= 12
                   or s.get("span_distance_lines", 0) >= 8)
        s["labels"] = {
            "categories": cats,
            "violations": s["expect_kinds"] or ["none"],
            "placement": "distant" if distant else "adjacent",
            "sub": s.get("sub", ""),
            "tier": tier,
        }
    return rows


def scenarios():
    rows = []
    rows += f_functional_date()
    rows += f_functional_city()
    rows += f_functional_price()
    rows += f_polarity()
    rows += f_cycle()
    rows += f_relation()
    rows += f_interval()
    rows += f_range()
    rows += f_cardinality()
    rows += f_instruction()
    rows += f_spatial()
    rows += f_causal()
    rows += f_disjoint()
    rows += f_domain()
    rows += f_identity()
    rows += f_controls()
    return rows


def main():
    rows = attach_labels(scenarios())
    dest = POC_DIR / "corpus.json"
    dest.write_text(json.dumps({"scenarios": rows}, indent=1))
    fams, cats, subs = {}, {}, {}
    for r in rows:
        fams.setdefault(r["family"], []).append(r)
        subs.setdefault((r["family"], r["sub"]), 0)
        subs[(r["family"], r["sub"])] += 1
        for c in r["labels"]["categories"]:
            cats[c] = cats.get(c, 0) + 1
    print(f"corpus.json written: {len(rows)} scenarios, "
          f"{len(fams)} families")
    for f, rs in sorted(fams.items()):
        distant = sum(1 for r in rs if r["labels"]["placement"] == "distant")
        sublist = sorted({r['sub'] for r in rs})
        print(f"  {f:20s} {len(rs):4d}  distant={distant:3d}  "
              f"subs={','.join(sublist)}")
    small = {k: v for k, v in subs.items() if v < 30}
    assert not small, f"sub-variants under n=30: {small}"
    multi = sum(1 for r in rows if len(r["labels"]["categories"]) > 1)
    print(f"\nBy category (multi-label; {multi} samples carry 2+):")
    for c, n in sorted(cats.items(), key=lambda kv: -kv[1]):
        print(f"  {c:15s} {n:4d}")
    ids = [r["id"] for r in rows]
    assert len(ids) == len(set(ids)), "duplicate scenario ids"
    tiers = {}
    for r in rows:
        tiers[r["labels"]["tier"]] = tiers.get(r["labels"]["tier"], 0) + 1
    print(f"\nTiers: smoke-only={tiers.get('smoke', 0)}, "
          f"standard-extra={tiers.get('standard', 0)}, "
          f"full-extra={tiers.get('full', 0)} "
          f"(smoke run = {tiers.get('smoke', 0)}, standard run = "
          f"{tiers.get('smoke', 0) + tiers.get('standard', 0)}, "
          f"full run = {len(rows)})")


if __name__ == "__main__":
    main()
