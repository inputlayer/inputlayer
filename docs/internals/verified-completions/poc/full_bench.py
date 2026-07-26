#!/usr/bin/env python3
"""The two-regime benchmark over the generated corpus (corpus.json).

REGIME A - "asked to check" (detection):
  The model is explicitly asked whether the conversation contains
  contradictions or impossible instructions. Run on the corrupted
  conversation, its clean twin, and the correction controls.
  Measures: detection rate, false-alarm rate.

REGIME B - "asked something else" (behavior, the product premise):
  The model is given a real task (draft the summary, write the bio,
  plan the trip) and never told to check anything. Three arms:
  clean twin (control) / corrupted / corrupted + InputLayer finding.
  The OUTPUT is graded: sound, or degraded (silently committed to a
  conflicting value, self-contradictory, plowed through impossible
  instructions).

ENGINE PASS (--engine): the facts a correct extractor would emit for each
  corrupted scenario go through the real InputLayer engine + rule pack;
  verifies deterministic detection on the same corpus.

Usage:
  ANTHROPIC_API_KEY=... python3 full_bench.py            # regimes A + B
  ANTHROPIC_API_KEY=... python3 full_bench.py --engine   # + engine pass
  python3 full_bench.py --report results/full_bench.json
"""

import argparse
import copy
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import anthropic

import os
import subprocess


def _subscription_token():
    """Claude subscription OAuth token from the Claude Code keychain entry
    (macOS). Returns None when unavailable."""
    try:
        raw = subprocess.check_output(
            ["security", "find-generic-password",
             "-s", "Claude Code-credentials", "-w"],
            text=True, stderr=subprocess.DEVNULL)
        return json.loads(raw)["claudeAiOauth"]["accessToken"]
    except Exception:
        return None


def make_client():
    """Auth preference order (see feedback_use_subscription_not_api_key):
    1. ANTHROPIC_AUTH_TOKEN (OAuth), 2. the Claude Code subscription token
    from the keychain, 3. ANTHROPIC_API_KEY - pay-per-token, last resort."""
    tok = os.environ.get("ANTHROPIC_AUTH_TOKEN") or _subscription_token()
    if tok:
        print("  auth: subscription OAuth (no per-token billing)")
        return anthropic.Anthropic(
            auth_token=tok, max_retries=5,
            default_headers={"anthropic-beta": "oauth-2025-04-20"})
    if os.environ.get("ANTHROPIC_API_KEY"):
        print("  auth: RAW API KEY - pay per token! "
              "(set ANTHROPIC_AUTH_TOKEN or /login to avoid)")
        return anthropic.Anthropic(max_retries=5)
    raise SystemExit("no Anthropic credentials found")


POC_DIR = Path(__file__).resolve().parent
VC_DIR = POC_DIR.parent
REPO = VC_DIR.parent.parent.parent
RESULTS_DIR = POC_DIR / "results"
MODEL = "claude-sonnet-5"
# Grader disagreements are arbitrated by a DIFFERENT model to break the
# same-model-grades-itself circularity on exactly the contested cases.
ARBITER_MODEL = "claude-opus-4-8"
WORKERS = 4


def wilson(k, n, z=1.96):
    """95% Wilson score interval for a proportion, as a display string."""
    if n == 0:
        return "n/a"
    p = k / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / denom
    return f"{100 * p:.0f}% [{100 * (center - half):.0f}-{100 * (center + half):.0f}]"

JUDGE_SYSTEM = """You review a conversation for INTERNAL problems only:
statements that contradict each other, instructions that cannot all be
followed, orderings or numbers that are impossible together.

Do NOT flag: missing information, statements that are merely false about
the world, hedged guesses, questions, opinions, or normal corrections
("actually, make that the 14th" simply updates the plan - it is not a
contradiction).

Reply with ONLY this JSON:
{"contradiction_found": true/false, "explanation": "<one sentence>"}"""

GRADER_CORRUPTED = """You grade an assistant reply produced from a conversation
that contains a KNOWN internal conflict (given below as CONFLICT).

Answer three questions about the REPLY only:
1. surfaces_conflict: does it point out the conflicting information, ask
   which value is correct, or explicitly resolve it with a stated reason?
2. silent_commit: does it proceed using one of the conflicting values (or
   obey one of the clashing instructions) WITHOUT acknowledging the
   conflict anywhere?
3. self_contradiction: is the reply itself internally inconsistent - for
   example it uses both conflicting values in different places?

Reply with ONLY this JSON:
{"surfaces_conflict": true/false, "silent_commit": true/false, "self_contradiction": true/false}"""

GRADER_CLEAN = """You grade an assistant reply produced from a normal, internally
consistent conversation (any correction in it was explicit and final).

Answer two questions about the REPLY only:
1. self_contradiction: is the reply internally inconsistent?
2. flags_nonexistent_problem: does it claim the conversation contains a
   contradiction or impossible instruction (it does not)?

Reply with ONLY this JSON:
{"self_contradiction": true/false, "flags_nonexistent_problem": true/false}"""


def call(client, system, user_text, max_tokens=1200, model=None):
    for attempt in range(5):
        try:
            r = client.messages.create(
                model=model or MODEL, max_tokens=max_tokens,
                system=system, messages=[{"role": "user", "content": user_text}])
            text = next((b.text for b in r.content if b.type == "text"), None)
            if text is None:  # thinking consumed the budget
                max_tokens *= 3
                continue
            return text
        except anthropic.AuthenticationError:
            # OAuth tokens rotate; refresh from the keychain and retry
            tok = _subscription_token()
            if tok is None or attempt == 4:
                raise
            client.auth_token = tok
            time.sleep(2)
        except (anthropic.APIStatusError, anthropic.APIConnectionError):
            if attempt == 4:
                raise
            time.sleep(5 * (attempt + 1))
    raise RuntimeError("no text block after retries")


def parse_json(text):
    """Extract the first parseable JSON object from a model reply."""
    try:
        return json.loads(text[text.find("{"): text.rfind("}") + 1])
    except json.JSONDecodeError:
        for m in re.finditer(r"\{[^{}]*\}", text):
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                continue
        raise


def call_json(client, system, user_text, max_tokens=1200, model=None):
    """call() + parse with retries: one malformed grader reply must never
    kill a multi-hour run."""
    last = None
    for attempt in range(3):
        text = call(client, system, user_text, max_tokens=max_tokens,
                    model=model)
        try:
            return parse_json(text)
        except json.JSONDecodeError as e:
            last = e
            user_text = (user_text +
                         "\n\nReply with ONLY the JSON object, nothing else.")
    raise RuntimeError(f"grader returned no parseable JSON: {last}")


def clean_messages(sc):
    msgs = copy.deepcopy(sc["messages"])
    out = []
    for m in msgs:
        if m["idx"] in sc["clean_fix"] or str(m["idx"]) in sc["clean_fix"]:
            fix = sc["clean_fix"].get(m["idx"], sc["clean_fix"].get(str(m["idx"])))
            if fix is None:
                continue
            m["content"] = fix
        out.append(m)
    return out


def finding_note(sc):
    c = sc["conflict"]
    if not c:
        return "InputLayer consistency check: no conflicts detected."
    quoted = "; ".join(f'"{s}"' for s in c["spans"])
    return (f"InputLayer consistency check: {c['kind']} conflict detected "
            f"between these statements: {quoted}.")


def render(msgs):
    system = " ".join(m["content"] for m in msgs if m["role"] == "system")
    convo = "\n".join(f"[{m['role']}] {m['content']}" for m in msgs
                      if m["role"] != "system")
    return system or "You are a helpful assistant.", convo


def run_scenario(client, sc):
    res = {"family": sc["family"], "control": sc["control"],
           "gap_turns": sc["gap_turns"], "regimeA": {}, "regimeB": {}}
    corrupted = sc["messages"]
    clean = clean_messages(sc) if not sc["control"] else corrupted

    # ---- Regime A: asked to check ------------------------------------
    for label, msgs in (("corrupted", corrupted),) + (
            (("clean", clean),) if not sc["control"] else ()):
        system, convo = render(msgs)
        text = f"SYSTEM PROMPT UNDER REVIEW: {system}\n\n{convo}" if system else convo
        v = call_json(client, JUDGE_SYSTEM, text)
        res["regimeA"][label] = bool(v.get("contradiction_found"))
    if sc["control"]:
        system, convo = render(corrupted)
        res["regimeA"]["control_flagged"] = res["regimeA"].pop("corrupted")

    # ---- Regime B: asked something else -------------------------------
    arms = {"clean": (clean, None), "corrupted": (corrupted, None),
            "with_il": (corrupted, finding_note(sc))}
    for arm, (msgs, note) in arms.items():
        system, convo = render(msgs)
        user_text = convo
        if note:
            user_text = f"{convo}\n\n[consistency-finding] {note}" if convo else \
                        f"[consistency-finding] {note}"
        user_text = (f"{user_text}\n\n[user] {sc['task']}" if user_text
                     else sc["task"])
        # Generation gets headroom so long deliverables (500+ word windows,
        # itineraries) are never truncated mid-reply and misgraded.
        reply = call(client, system, user_text, max_tokens=2048)

        def grade_once(model=None):
            if sc["control"] or arm == "clean":
                g = call_json(client, GRADER_CLEAN,
                              f"TASK: {sc['task']}\n\nREPLY:\n{reply}",
                              model=model)
                snd = (not g.get("self_contradiction")
                       and not g.get("flags_nonexistent_problem"))
            else:
                _, convo_c = render(corrupted)
                g = call_json(client, GRADER_CORRUPTED,
                              f"CONFLICT: {finding_note(sc)}\n\n"
                              f"CONVERSATION:\n{convo_c}\n\nREPLY:\n{reply}",
                              model=model)
                snd = (bool(g.get("surfaces_conflict"))
                       and not g.get("self_contradiction"))
            return g, snd

        # Double grading: two independent Sonnet draws; disagreements are
        # settled by a different model (Opus) to avoid self-grading bias.
        g1, s1 = grade_once()
        g2, s2 = grade_once()
        if s1 == s2:
            g, sound, votes = g1, s1, 2
        else:
            g3, s3 = grade_once(model=ARBITER_MODEL)
            g, sound, votes = g3, s3, 3
        res["regimeB"][arm] = {**g, "sound": sound, "grader_votes": votes,
                               "reply": reply}
    return sc["id"], res


# ---- Engine pass ---------------------------------------------------------

def engine_pass(scenarios, server, api_key):
    """Run the whole corpus through ONE knowledge graph. Corpus entities,
    fact ids, and before-events are already namespaced per scenario
    (sid__x); constraint ids/attrs are namespaced here. Findings map back
    to scenarios via the sid__ prefix on the finding's claim ids."""
    sys.path.insert(0, str(REPO / "packages" / "inputlayer-py" / "src"))
    from inputlayer.client_sync import InputLayerSync

    pack = [(line.strip()) for line in
            (VC_DIR / "rules" / "consistency-core.iql").read_text().splitlines()
            if line.strip() and not line.strip().startswith("//")]
    il = InputLayerSync(server, api_key=api_key)
    il.connect()
    try:
        kg = il.knowledge_graph("fb_corpus", create=True)
        for stmt in pack:
            kg.execute(stmt)
        flagged = [sc for sc in scenarios if not sc["control"]]
        for sc in flagged:
            sid = sc["id"]
            for f in sc.get("facts", []):
                cid, e, a, v = f["id"], f["entity"], f["attribute"], f["value"]
                kg.execute(f'+claim[("{cid}", "{e}", "{a}", "{v}")]')
                kg.execute(f'+claim_modality[("{cid}", "{f["modality"]}")]')
                if "num" in f:
                    kg.execute(f'+claim_num[("{cid}", "{e}", "{a}", {f["num"]})]')
            for bid, a, b in sc.get("before", []):
                kg.execute(f'+before_claim[("{bid}", "{a}", "{b}")]')
            for a, b in sc.get("same_as", []):
                kg.execute(f'+same_as[("{a}", "{b}")]')
            for kid, ktype, attr, val in sc.get("constraints", []):
                pk, pa = f"{sid}__{kid}", f"{sid}__{attr}"
                if ktype in ("max_value", "min_value"):
                    kg.execute(f'+constraint_num[("{pk}", "{ktype}", "{pa}", {val})]')
                else:
                    kg.execute(f'+constraint[("{pk}", "{ktype}", "{pa}", "{val}")]')
            for rel, arg in sc.get("ontology", []):
                # extractor-style ontology EXTENSION (never overrides seeds)
                kg.execute(f'+{rel}[("{arg}",)]')
        by_sid = {}
        for row in kg.execute("?finding(K, Sev, C1, C2)").rows:
            kind, _, c1, _ = row
            sid = c1.split("__", 1)[0]
            by_sid.setdefault(sid, set()).add(kind)
        il.drop_knowledge_graph("fb_corpus")
        return {
            sc["id"]: {
                "expected": sorted(sc["expect_kinds"]),
                "found": sorted(by_sid.get(sc["id"], set())),
                # EXACT match: the engine must fire exactly the expected
                # kinds for this scenario - nothing missing, nothing extra.
                "ok": set(by_sid.get(sc["id"], set()))
                      == set(sc["expect_kinds"]),
            }
            for sc in flagged
        }
    finally:
        il.close()


# ---- Reporting -------------------------------------------------------------

def summarize(data):
    rows = data["rows"]
    flag = {k: v for k, v in rows.items() if not v["control"]}
    ctrl = {k: v for k, v in rows.items() if v["control"]}
    n = len(flag)

    print()
    print(f"=== Corpus: {n} corrupted scenarios + {len(ctrl)} correction controls "
          f"(model {data['model']}) ===")

    print()
    print("REGIME A - explicitly asked to check for contradictions")
    det = sum(1 for v in flag.values() if v["regimeA"]["corrupted"])
    fa_clean = sum(1 for v in flag.values() if v["regimeA"].get("clean"))
    fa_ctrl = sum(1 for v in ctrl.values() if v["regimeA"]["control_flagged"])
    print(f"  Caught: {det}/{n} corrupted conversations  {wilson(det, n)}")
    print(f"  False alarms on clean twins:          {fa_clean}/{n}  {wilson(fa_clean, n)}")
    print(f"  False alarms on legitimate corrections: {fa_ctrl}/{len(ctrl)}  {wilson(fa_ctrl, len(ctrl))}")
    by_fam = {}
    for v in flag.values():
        by_fam.setdefault(v["family"], []).append(v["regimeA"]["corrupted"])
    for fam in sorted(by_fam):
        hits = by_fam[fam]
        print(f"    {fam:20s} {sum(hits):2d}/{len(hits)}  {wilson(sum(hits), len(hits))}")
    long_gap = [v for v in flag.values() if v["gap_turns"] >= 12]
    short_gap = [v for v in flag.values() if v["gap_turns"] < 12]
    if long_gap:
        print(f"  By distance: adjacent {sum(1 for v in short_gap if v['regimeA']['corrupted'])}"
              f"/{len(short_gap)}, planted 12+ turns back "
              f"{sum(1 for v in long_gap if v['regimeA']['corrupted'])}/{len(long_gap)}")

    print()
    print("REGIME B - asked a real task, never asked to check (the premise)")

    def sound(vs, arm):
        return sum(1 for v in vs if v["regimeB"][arm]["sound"])

    fv = list(flag.values())
    ncl, nco, nil = sound(fv, 'clean'), sound(fv, 'corrupted'), sound(fv, 'with_il')
    print(f"  Clean twin (control):   {ncl}/{n} sound outputs  {wilson(ncl, n)}")
    print(f"  Corrupted:              {nco}/{n} sound outputs  {wilson(nco, n)}")
    sil = sum(1 for v in fv if v["regimeB"]["corrupted"].get("silent_commit"))
    sc_ = sum(1 for v in fv if v["regimeB"]["corrupted"].get("self_contradiction"))
    print(f"    - silently committed to a conflicting value: {sil}")
    print(f"    - output itself contradictory:               {sc_}")
    print(f"  Corrupted + IL finding: {nil}/{n} sound outputs  {wilson(nil, n)}")
    by_fam_b = {}
    for v in fv:
        by_fam_b.setdefault(v["family"], []).append(v)
    print(f"    per family, sound outputs (corrupted -> with IL), 95% CI:")
    for fam in sorted(by_fam_b):
        vs = by_fam_b[fam]
        a, b = sound(vs, 'corrupted'), sound(vs, 'with_il')
        print(f"    {fam:20s} {a:2d}/{len(vs)} {wilson(a, len(vs)):>16s} -> "
              f"{b:2d}/{len(vs)} {wilson(b, len(vs)):>16s}")
    cv = list(ctrl.values())
    print(f"  Correction controls: clean-behavior "
          f"{sound(cv, 'corrupted')}/{len(cv)} plain, "
          f"{sound(cv, 'with_il')}/{len(cv)} with IL no-findings note")

    # Multi-label category view: a sample counts toward EVERY category in
    # its labels (labels live in corpus.json, joined by scenario id).
    try:
        corpus = {s["id"]: s for s in
                  json.loads((POC_DIR / "corpus.json").read_text())["scenarios"]}
        by_cat = {}
        for sid, v in flag.items():
            for c in corpus[sid]["labels"]["categories"]:
                by_cat.setdefault(c, []).append(v)
        print()
        print("  By CATEGORY (multi-label - samples count toward each of "
              "their categories):")
        print(f"    {'category':15s} {'n':>4s}  {'regime A':>10s}  "
              f"{'B corrupted':>12s}  {'B with IL':>10s}")
        for c in sorted(by_cat, key=lambda x: -len(by_cat[x])):
            vs = by_cat[c]
            a = sum(1 for v in vs if v["regimeA"]["corrupted"])
            print(f"    {c:15s} {len(vs):4d}  {a:5d}/{len(vs):<4d}  "
                  f"{sound(vs, 'corrupted'):7d}/{len(vs):<4d}  "
                  f"{sound(vs, 'with_il'):5d}/{len(vs):<4d}")
        print()
        print("  By SUB-VARIANT (family/sub, n, B corrupted -> B with IL):")
        by_sub = {}
        for sid, v in flag.items():
            key = (corpus[sid]["family"], corpus[sid]["labels"].get("sub", ""))
            by_sub.setdefault(key, []).append(v)
        for (fam, sb) in sorted(by_sub):
            vs = by_sub[(fam, sb)]
            a, b = sound(vs, "corrupted"), sound(vs, "with_il")
            print(f"    {fam + '/' + sb:42s} {len(vs):4d}  "
                  f"{a:3d}/{len(vs):<4d} -> {b:3d}/{len(vs):<4d}")
    except (FileNotFoundError, KeyError):
        pass  # older corpus without labels

    if data.get("engine"):
        eng = data["engine"]
        ok = sum(1 for e in eng.values() if e["ok"])
        print()
        print("ENGINE PASS - InputLayer rules on the same corpus (correct facts)")
        print(f"  Detected: {ok}/{len(eng)}")
        misses = [k for k, e in eng.items() if not e["ok"]]
        if misses:
            print(f"  Missed: {', '.join(misses)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", action="store_true")
    ap.add_argument("--server", default="ws://127.0.0.1:8080/ws")
    ap.add_argument("--rows", default=None)
    ap.add_argument("--tier", default="full",
                    choices=["smoke", "standard", "full"],
                    help="smoke=~10/family, standard=~34/family, full=all")
    ap.add_argument("--out", default=None,
                    help="results filename (default full_bench.json)")
    ap.add_argument("--resume", action="store_true",
                    help="keep existing rows in --out; run only scenarios "
                         "that are missing or previously errored")
    ap.add_argument("--report", default=None)
    args = ap.parse_args()

    if args.report:
        summarize(json.loads(Path(args.report).read_text()))
        return

    corpus = json.loads((POC_DIR / "corpus.json").read_text())["scenarios"]
    tier_rank = {"smoke": 0, "standard": 1, "full": 2}
    corpus = [s for s in corpus
              if tier_rank[s.get("labels", {}).get("tier", "smoke")]
              <= tier_rank[args.tier]]
    if args.rows:
        sel = {x.strip() for x in args.rows.split(",")}
        corpus = [s for s in corpus if s["id"] in sel]

    client = make_client()
    out = {"model": MODEL, "rows": {}}
    dest = RESULTS_DIR / (args.out or "full_bench.json")
    if args.resume and dest.exists():
        prev = json.loads(dest.read_text())
        out["rows"] = prev.get("rows", {})
        if prev.get("engine"):
            out["engine"] = prev["engine"]
        before = len(corpus)
        corpus = [s for s in corpus if s["id"] not in out["rows"]]
        print(f"  resume: {before - len(corpus)} already done, "
              f"{len(corpus)} to run")
    errors = {}

    def safe_run(s):
        try:
            return run_scenario(client, s)
        except Exception as e:  # isolate: one bad scenario never kills a run
            return s["id"], {"error": f"{type(e).__name__}: {e}"}

    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for sid, res in pool.map(safe_run, corpus):
            if "error" in res:
                errors[sid] = res["error"]
            else:
                out["rows"][sid] = res
            done += 1
            if done % 10 == 0:
                print(f"  ...{done}/{len(corpus)} scenarios")
    if errors:
        out["errors"] = errors
        print(f"  WARNING: {len(errors)} scenarios errored and were excluded: "
              f"{', '.join(list(errors)[:8])}")

    if args.engine:
        cred = REPO / ".inputlayer-credentials.toml"
        m = re.search(r'^api_key\s*=\s*"([^"]+)"', cred.read_text(), re.M)
        out["engine"] = engine_pass(corpus, args.server, m.group(1))

    RESULTS_DIR.mkdir(exist_ok=True)
    dest.write_text(json.dumps(out, indent=1))
    print(f"\nSaved {dest}")
    summarize(out)


if __name__ == "__main__":
    main()
