#!/usr/bin/env python3
"""Per-scenario verification ledger: every sample individually checked.

For EACH of the corpus scenarios this verifies, individually:

  spans_ok       every ground-truth conflicting span is a verbatim
                 substring of the conversation
  clean_ok       the clean twin actually differs from the corrupted
                 conversation (controls: trivially true)
  labels_ok      labels are present, categories in vocabulary, tier and
                 placement set, sub named
  engine_ok      the engine fired EXACTLY the expected finding kinds for
                 this scenario's facts - nothing missing, nothing extra
                 (controls: skipped, no facts)
  llm_ok         benchmark results exist for this scenario: regime A
                 verdicts present, all three regime B arms generated,
                 graded, with vote counts and non-empty replies

A scenario is VERIFIED only if every applicable check passes. The ledger
is written to results/verification_ledger.json; the exit code is nonzero
if any scenario is unverified, so this can gate CI.

Usage:
  python3 verify_each.py --results results/full_bench.json [--engine]
"""

import argparse
import json
import re
import sys
from pathlib import Path

POC_DIR = Path(__file__).resolve().parent
REPO = POC_DIR.parent.parent.parent.parent

sys.path.insert(0, str(POC_DIR))
from corpus import CATEGORY_VOCABULARY  # noqa: E402
from full_bench import engine_pass  # noqa: E402


def check_scenario(sc, results_row, engine_row):
    checks = {}
    text = " ".join(m["content"] for m in sc["messages"])
    checks["spans_ok"] = (True if not sc["conflict"] else
                          all(sp in text for sp in sc["conflict"]["spans"]))
    if sc["control"]:
        checks["clean_ok"] = True
    else:
        fixes = {int(k): v for k, v in
                 ((int(k) if isinstance(k, str) else k, v)
                  for k, v in sc["clean_fix"].items())} if sc["clean_fix"] else {}
        checks["clean_ok"] = bool(fixes) and all(
            (v is None) or (v != next(m["content"] for m in sc["messages"]
                                      if m["idx"] == k))
            for k, v in fixes.items())
    lab = sc.get("labels", {})
    checks["labels_ok"] = (bool(lab.get("categories"))
                           and all(c in CATEGORY_VOCABULARY
                                   for c in lab.get("categories", []))
                           and lab.get("placement") in ("adjacent", "distant")
                           and lab.get("tier") in ("smoke", "standard", "full")
                           and "sub" in lab)
    if sc["control"]:
        checks["engine_ok"] = True  # no facts; nothing may fire
    else:
        checks["engine_ok"] = bool(engine_row and engine_row.get("ok"))
    if results_row is None or "error" in (results_row or {}):
        checks["llm_ok"] = False
    else:
        ra = results_row.get("regimeA", {})
        rb = results_row.get("regimeB", {})
        ra_ok = (("control_flagged" in ra) if sc["control"]
                 else ("corrupted" in ra and "clean" in ra))
        rb_ok = all(arm in rb and "sound" in rb[arm]
                    and rb[arm].get("grader_votes") in (2, 3)
                    and bool(rb[arm].get("reply", "").strip())
                    for arm in ("clean", "corrupted", "with_il"))
        checks["llm_ok"] = ra_ok and rb_ok
    return checks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="results/full_bench.json")
    ap.add_argument("--engine", action="store_true",
                    help="run the engine pass fresh instead of reusing "
                         "the results file's engine section")
    ap.add_argument("--server", default="ws://127.0.0.1:8080/ws")
    args = ap.parse_args()

    corpus = json.loads((POC_DIR / "corpus.json").read_text())["scenarios"]
    results_path = POC_DIR / args.results
    results = (json.loads(results_path.read_text())
               if results_path.exists() else {"rows": {}})

    engine = results.get("engine", {})
    if args.engine or not engine:
        cred = REPO / ".inputlayer-credentials.toml"
        m = re.search(r'^api_key\s*=\s*"([^"]+)"', cred.read_text(), re.M)
        engine = engine_pass(corpus, args.server, m.group(1))

    ledger = {}
    for sc in corpus:
        checks = check_scenario(sc, results["rows"].get(sc["id"]),
                                engine.get(sc["id"]))
        ledger[sc["id"]] = {**checks, "verified": all(checks.values()),
                            "family": sc["family"], "sub": sc["sub"]}

    dest = POC_DIR / "results" / "verification_ledger.json"
    dest.parent.mkdir(exist_ok=True)
    dest.write_text(json.dumps(
        {"engine": engine, "ledger": ledger}, indent=1))

    n = len(ledger)
    verified = sum(1 for v in ledger.values() if v["verified"])
    print(f"VERIFICATION LEDGER: {verified}/{n} scenarios individually verified")
    for axis in ("spans_ok", "clean_ok", "labels_ok", "engine_ok", "llm_ok"):
        bad = [k for k, v in ledger.items() if not v[axis]]
        print(f"  {axis:10s} {n - len(bad):5d}/{n}"
              + (f"   failing: {', '.join(bad[:6])}"
                 + (" ..." if len(bad) > 6 else "") if bad else ""))
    unverified = [k for k, v in ledger.items() if not v["verified"]]
    if unverified:
        print(f"UNVERIFIED ({len(unverified)}): {', '.join(unverified[:20])}"
              + (" ..." if len(unverified) > 20 else ""))
        sys.exit(1)
    print("ALL SCENARIOS INDIVIDUALLY VERIFIED")


if __name__ == "__main__":
    main()
